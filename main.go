/*
 * Copyright (c) 2025, Natacha Porté
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package main

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/nats-io/nats.go"
	"github.com/pelletier/go-toml/v2"
	"github.com/thoj/go-ircevent"
)

type command struct {
	name string
	arg  string
}

func main() {
	setVersion()

	config_file := "natsim.toml"
	if len(os.Args) > 1 {
		config_file = os.Args[1]
	}

	im, err := NewNatsIM(config_file)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("natsim " + version + " started")

	im.irc.Loop()
}

/**************** Construction ****************/

type LineMark struct {
	Start string
	Mid   string
	End   string
}

type IrcConfig struct {
	Channel    string
	Server     string
	Nick       string
	Cmd        LineMark
	Send       LineMark
	Show       LineMark
	ShowReply  *LineMark
	ShowHeader *LineMark
	MaxLine    int
	ContSuffix string
	ContPrefix string
	AntiFlood  antiflood
	Filter     []FilterElement
}

type LogConfig struct {
	SqlDriver     string
	SqlConnection string
	Filter        []FilterElement
}

type NatsConfig struct {
	Server   string
	NkeySeed string
	Subjects []string
	Filter   []FilterElement
}

type NatsIM struct {
	Irc  IrcConfig
	Log  LogConfig
	Nats NatsConfig

	irc            *irc.Connection
	nc             *nats.Conn
	db             *sql.DB
	ensureSubject  *sql.Stmt
	insertReceived *sql.Stmt
	insertRHeader  *sql.Stmt
	insertSent     *sql.Stmt
	cmdQueue       chan command
	ircQueue       chan string
	dropped        atomic.Uint32
}

func NewNatsIM(configPath string) (*NatsIM, error) {
	natsim := &NatsIM{
		Irc: IrcConfig{
			Nick: "natsim",
			Cmd: LineMark{
				Start: "!",
				Mid:   " ",
			},
			Send: LineMark{Mid: ": "},
			Show: LineMark{Mid: ": "},
		},
		Nats: NatsConfig{
			Subjects: []string{">"},
		},
	}

	f, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Println("Closing configuration", configPath, err)
		}
	}()

	d := toml.NewDecoder(f).DisallowUnknownFields()
	err = d.Decode(natsim)
	if err != nil {
		var details *toml.StrictMissingError
		if errors.As(err, &details) {
			for _, missing := range details.Errors {
				log.Println(missing)
			}
		}
		return nil, err
	}

	if natsim.Irc.MaxLine > 0 && len(natsim.Irc.ContPrefix)+len(natsim.Irc.ContSuffix) >= natsim.Irc.MaxLine {
		natsim.Irc.ContPrefix = ""
		natsim.Irc.ContSuffix = ""
	}

	if natsim.Log.SqlDriver != "" {
		if err := natsim.logInit(); err != nil {
			natsim.Close()
			return nil, err
		}
	}

	natsim.cmdQueue = make(chan command, 10)
	natsim.ircQueue = make(chan string, 10)

	natsim.irc = irc.IRC(natsim.Irc.Nick, "natsim")
	natsim.irc.AddCallback("001", natsim.ircJoin)
	natsim.irc.AddCallback("366", natsim.ircJoined)
	natsim.irc.AddCallback("PRIVMSG", natsim.ircReceive)

	err = natsim.irc.Connect(natsim.Irc.Server)
	if err != nil {
		natsim.Close()
		return nil, err
	}

	go natsim.doCommands()
	go natsim.ircSender()

	return natsim, nil
}

func (natsim *NatsIM) Close() {
	if natsim.irc != nil {
		natsim.irc.Quit()
		natsim.irc = nil
	}

	if natsim.nc != nil {
		natsim.nc.Close()
		natsim.nc = nil
	}

	if natsim.ensureSubject != nil {
		if err := natsim.ensureSubject.Close(); err != nil {
			log.Println("Close ensureSubject:", err)
		}
		natsim.ensureSubject = nil
	}

	if natsim.insertReceived != nil {
		if err := natsim.insertReceived.Close(); err != nil {
			log.Println("Close insertReceived:", err)
		}
		natsim.insertReceived = nil
	}

	if natsim.insertRHeader != nil {
		if err := natsim.insertRHeader.Close(); err != nil {
			log.Println("Close insertRHeader:", err)
		}
		natsim.insertRHeader = nil
	}

	if natsim.insertSent != nil {
		if err := natsim.insertSent.Close(); err != nil {
			log.Println("Close insertSent:", err)
		}
		natsim.insertSent = nil
	}

	if natsim.db != nil {
		if err := natsim.db.Close(); err != nil {
			log.Println("Close log DB:", err)
		}
		natsim.db = nil
	}

	close(natsim.cmdQueue)
	close(natsim.ircQueue)
}

/**************** Command Goroutine ****************/

func (natsim *NatsIM) doCommands() {
	for {
		cmd, ok := <-natsim.cmdQueue
		if !ok {
			return
		}

		switch cmd.name {
		case "quit":
			log.Println("Quit command", cmd.arg)
			natsim.irc.QuitMessage = cmd.arg
			natsim.Close()

		case "version":
			natsim.ircSendf("natsim %s", version)

		default:
			natsim.ircSendf("Unknown command %q", cmd.name)
		}
	}
}

/**************** IRC Callbacks ****************/

func (natsim *NatsIM) ircJoin(e *irc.Event) {
	natsim.irc.Join(natsim.Irc.Channel)
}

func (natsim *NatsIM) ircJoined(e *irc.Event) {
	optSeed, err := nats.NkeyOptionFromSeed(natsim.Nats.NkeySeed)
	if err != nil {
		natsim.ircSendError("NkeyOptionFromSeed", err)
		return
	}

	natsim.nc, err = nats.Connect(natsim.Nats.Server,
		optSeed,
		nats.ConnectHandler(natsim.natsConnected),
		nats.DisconnectErrHandler(natsim.natsDisconnected),
		nats.ReconnectHandler(natsim.natsReconnected),
		nats.ReconnectErrHandler(natsim.natsReconnectErr))
	if err != nil {
		natsim.ircSendError("Connect", err)
		return
	}

	for _, subject := range natsim.Nats.Subjects {
		if _, err := natsim.nc.Subscribe(subject, natsim.natsReceive); err != nil {
			natsim.ircSendError("Subscribe", err)
		}
	}
}

func (natsim *NatsIM) ircReceive(e *irc.Event) {
	msg := e.Message()
	if name, arg, found := unpackMark(natsim.Irc.Cmd, msg, true); found {
		natsim.cmdQueue <- command{name: name, arg: arg}
	} else if subject, data, found := unpackMark(natsim.Irc.Send, msg, false); found {
		if err := natsim.nc.Publish(subject, []byte(data)); err != nil {
			natsim.ircSendError("Publish", err)
		} else {
			natsim.logSent(subject, data)
		}
	}
}

func (natsim *NatsIM) ircSendError(context string, err error) {
	prefix := "[E] "
	if context != "" {
		prefix += context + ": "
	}
	natsim.ircSend(prefix + err.Error())
}

func (natsim *NatsIM) ircSend(s string) {
	select {
	case natsim.ircQueue <- s:
	default:
		natsim.dropped.Add(1)
	}
}

func (natsim *NatsIM) ircSendf(format string, a ...interface{}) {
	natsim.ircSend(fmt.Sprintf(format, a...))
}

func (natsim *NatsIM) ircSender() {
	var dropped uint32
	var nindex = 0
	floodend := make([]time.Time, natsim.Irc.AntiFlood.count)
	delay := natsim.Irc.AntiFlood.delay / time.Duration(natsim.Irc.AntiFlood.count)
	prev := time.Now()

	for {
		var lines []string

		if len(natsim.ircQueue) == 0 {
			dropped += natsim.dropped.Swap(0)
		}

		if dropped > 0 {
			select {
			case s, ok := <-natsim.ircQueue:
				if ok {
					lines = natsim.ircSplit(s)
				} else {
					return
				}
			case <-time.After(delay):
				dropped += natsim.dropped.Swap(0)
				lines = []string{fmt.Sprintf("Dropped %d messages", dropped)}
				dropped = 0
			}
		} else {
			s, ok := <-natsim.ircQueue
			if ok {
				lines = natsim.ircSplit(s)
			} else {
				return
			}
		}

		for _, line := range lines {
			if time.Until(floodend[nindex]) > 0 {
				time.Sleep(time.Until(prev.Add(delay)))
			}

			natsim.irc.Privmsg(natsim.Irc.Channel, line)

			prev = time.Now()
			floodend[nindex] = prev.Add(natsim.Irc.AntiFlood.delay)
			nindex = (nindex + 1) % natsim.Irc.AntiFlood.count
		}
	}
}

func (natsim *NatsIM) ircSplit(s string) []string {
	var result []string

	for _, line := range strings.Split(s, "\n") {
		if natsim.Irc.MaxLine <= 0 || len(line) < natsim.Irc.MaxLine {
			result = append(result, line)
		} else {
			for offset := 0; offset < len(line); {
				var buf strings.Builder
				l := len(line) - offset
				if offset > 0 {
					buf.WriteString(natsim.Irc.ContPrefix)
				}

				if buf.Len()+l <= natsim.Irc.MaxLine {
					buf.WriteString(line[offset:])
				} else {
					l = natsim.Irc.MaxLine - buf.Len() - len(natsim.Irc.ContSuffix)
					buf.WriteString(line[offset : offset+l])
					buf.WriteString(natsim.Irc.ContSuffix)
				}

				result = append(result, buf.String())
				offset += l
			}
		}
	}

	return result
}

/**************** Nats Callbacks ****************/

func (natsim *NatsIM) natsConnected(c *nats.Conn) {
	natsim.ircSend("Connected to " + c.ConnectedUrlRedacted())
}

func (natsim *NatsIM) natsDisconnected(c *nats.Conn, err error) {
	if err != nil {
		natsim.ircSendError("Disconnected", err)
	}
}

func (natsim *NatsIM) natsReceive(m *nats.Msg) {
	if !IsKept(m.Subject, m.Data, natsim.Nats.Filter, true) {
		return
	}

	if IsKept(m.Subject, m.Data, natsim.Log.Filter, true) {
		natsim.logReceived(m)
	}

	if !IsKept(m.Subject, m.Data, natsim.Irc.Filter, true) {
		return
	}

	var sb strings.Builder
	sb.WriteString(packMark(natsim.Irc.Show, m.Subject, string(m.Data)))

	if m.Reply != "" && natsim.Irc.ShowReply != nil {
		sb.WriteString(natsim.Irc.ShowReply.Start)
		sb.WriteString(m.Reply)
		sb.WriteString(natsim.Irc.ShowReply.End)
	}

	if natsim.Irc.ShowHeader != nil {
		for key, values := range m.Header {
			for _, value := range values {
				sb.WriteString(packMark(*natsim.Irc.ShowHeader, key, value))
			}
		}
	}

	natsim.ircSend(sb.String())
}

func (natsim *NatsIM) natsReconnected(c *nats.Conn) {
	natsim.ircSend("Reconnected to " + c.ConnectedUrlRedacted())
}

func (natsim *NatsIM) natsReconnectErr(c *nats.Conn, err error) {
	natsim.ircSendError("Reconnect", err)
}

/**************** Log to Database ****************/

//go:embed init.sql
var embeddedSQL embed.FS

func (natsim *NatsIM) logInit() error {
	if natsim.Log.SqlDriver == "" {
		return nil
	}

	var err error

	natsim.db, err = sql.Open(natsim.Log.SqlDriver, natsim.Log.SqlConnection)
	if err != nil {
		log.Println("sql.Open:", err)
		return err
	}

	var version int
	if err = natsim.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		log.Println("query user_verison", err)
		return err
	}

	switch version {
	case 0:
		initSQL, err := embeddedSQL.ReadFile("init.sql")
		if err != nil {
			log.Println("embedded.ReadFile:", err)
			return err
		}

		if _, err = natsim.db.Exec(string(initSQL)); err != nil {
			log.Println("Init log DB:", err)
			return err
		}

	case 1:

	default:
		log.Println("Unsupported database version:", version)
		return errors.New("unsupported database version")
	}

	natsim.ensureSubject, err = natsim.db.Prepare("INSERT INTO subjects(name) SELECT ? WHERE NOT EXISTS (SELECT 1 FROM subjects WHERE name = ?);")
	if err != nil {
		log.Println("Prepare ensureSubject:", err)
		return err
	}

	natsim.insertReceived, err = natsim.db.Prepare("INSERT INTO received(timestamp,subject_id,reply_subject_id,data) VALUES (?, (SELECT id FROM subjects WHERE name = ?), (SELECT id FROM subjects WHERE name = ?), ?);")
	if err != nil {
		log.Println("Prepare insertReceived:", err)
		return err
	}

	natsim.insertRHeader, err = natsim.db.Prepare("INSERT INTO received_headers_view(msg_id,key,value) VALUES (?,?,?);")
	if err != nil {
		log.Println("Prepare insertRHeader:", err)
		return err
	}

	natsim.insertSent, err = natsim.db.Prepare("INSERT INTO sent_view(timestamp,subject,data) VALUES (?,?,?);")
	if err != nil {
		log.Println("Prepare insertSent:", err)
		return err
	}

	return nil
}

func (natsim *NatsIM) logReceived(msg *nats.Msg) {
	if natsim.db == nil || natsim.insertReceived == nil {
		return
	}

	if _, err := natsim.ensureSubject.Exec(msg.Subject, msg.Subject); err != nil {
		natsim.ircSendError("ensureSubject.Exec", err)
		return
	}

	var reply sql.NullString
	if msg.Reply != "" {
		if _, err := natsim.ensureSubject.Exec(msg.Reply, msg.Reply); err != nil {
			natsim.ircSendError("ensureReply.Exec", err)
			return
		}
		reply = sql.NullString{String: msg.Reply, Valid: true}
	}

	t := float64(time.Now().UnixNano())/8.64e13 + 2440587.5
	if r, err := natsim.insertReceived.Exec(t, msg.Subject, reply, msg.Data); err != nil {
		natsim.ircSendError("insertReceived.Exec", err)
	} else if id, err := r.LastInsertId(); err != nil {
		natsim.ircSendError("LastInsertId", err)
	} else if id <= 0 {
		natsim.ircSendf("LastInsertId returned invalid id %d", id)
	} else {
		for key, values := range msg.Header {
			for _, value := range values {
				if _, err := natsim.insertRHeader.Exec(id, key, value); err != nil {
					natsim.ircSendf("insertRHeader(%q, %q): %s", key, value, err)
				}
			}
		}
	}
}

func (natsim *NatsIM) logSent(subject, data string) {
	if natsim.db == nil || natsim.insertSent == nil {
		return
	}

	t := float64(time.Now().UnixNano())/8.64e13 + 2440587.5
	if _, err := natsim.insertSent.Exec(t, subject, data); err != nil {
		natsim.ircSendError("insertSent.Exec", err)
	}
}

/**************** Filters ****************/

type FilterElement struct {
	Result bool
	Part   FilterPart
	Test   *regexp.Regexp
}

type FilterPart int

const (
	FilterSubject FilterPart = iota
	FilterData
)

func (element *FilterElement) Match(subject string, data []byte) bool {
	var b []byte
	switch element.Part {
	case FilterSubject:
		b = []byte(subject)
	case FilterData:
		b = data
	default:
		panic("Unexpected part")
	}

	return element.Test.Match(b)
}

func (element *FilterElement) String() string {
	r := "drop "
	if element.Result {
		r = "pass "
	}

	p := ""
	switch element.Part {
	case FilterSubject:
		p = "subject "
	case FilterData:
		p = "data "
	default:
		panic("Unexpected part")
	}

	return r + p + element.Test.String()
}

func (element *FilterElement) UnmarshalText(text []byte) error {
	s := string(text)

	switch {
	case strings.HasPrefix(s, "pass "):
		element.Result = true
		s = s[5:]
	case strings.HasPrefix(s, "drop "):
		element.Result = false
		s = s[5:]
	default:
		return fmt.Errorf("malformed filter %q", s)
	}

	switch {
	case strings.HasPrefix(s, "subject "):
		element.Part = FilterSubject
		s = s[8:]
	case strings.HasPrefix(s, "data "):
		element.Part = FilterData
		s = s[5:]
	default:
		return fmt.Errorf("bad filter part %q", s)
	}

	re, err := regexp.Compile(s)
	element.Test = re
	return err
}

func IsKept(subject string, data []byte, elements []FilterElement, base bool) bool {
	for _, element := range elements {
		if element.Match(subject, data) {
			return element.Result
		}
	}

	return base
}

/**************** Tools ****************/

type antiflood struct {
	count int
	delay time.Duration
}

func (af *antiflood) UnmarshalText(text []byte) error {
	if before, after, found := strings.Cut(string(text), "/"); found {
		if n, err := strconv.Atoi(before); err != nil {
			return err
		} else {
			af.count = n
		}

		if d, err := time.ParseDuration(after); err != nil {
			return err
		} else {
			af.delay = d
		}

	} else if d, err := time.ParseDuration(string(text)); err != nil {
		return err
	} else {
		af.count = 1
		af.delay = d
	}

	return nil
}

func packMark(mark LineMark, name, arg string) string {
	return mark.Start + name + mark.Mid + arg + mark.End
}

func unpackMark(mark LineMark, line string, optional bool) (string, string, bool) {
	if strings.HasPrefix(line, mark.Start) && strings.HasSuffix(line, mark.End) {
		inside := line[len(mark.Start) : len(line)-len(mark.End)]
		if mark.Mid == "" {
			return inside, "", true
		} else if name, arg, found := strings.Cut(inside, mark.Mid); found {
			return name, arg, true
		} else {
			return inside, "", optional
		}
	} else {
		return "", "", false
	}
}

var version = "(unknown)"

func setVersion() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	version = info.Main.Version

	if version == "(devel)" {
		vcs := ""
		rev := ""
		dirty := ""
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs":
				vcs = setting.Value + "-"
			case "vcs.revision":
				rev = setting.Value[0:8]
			case "vcs.modified":
				if setting.Value == "true" {
					dirty = "*"
				}
			}
		}

		if rev != "" {
			version = vcs + rev + dirty
		}
	}
}
