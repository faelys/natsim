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
	"errors"
	"log"
	"os"
	"runtime/debug"
	"strings"

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
	MaxLine    int
	ContSuffix string
	ContPrefix string
}

type NatsConfig struct {
	Server   string
	NkeySeed string
	Subjects []string
}

type NatsIM struct {
	Irc  IrcConfig
	Nats NatsConfig

	irc      *irc.Connection
	nc       *nats.Conn
	cmdQueue chan command
	ircQueue chan string
	buf      strings.Builder
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

	natsim.cmdQueue = make(chan command, 10)
	natsim.ircQueue = make(chan string, 10)

	opt, err := nats.NkeyOptionFromSeed(natsim.Nats.NkeySeed)
	if err != nil {
		return nil, err
	}

	natsim.nc, err = nats.Connect(natsim.Nats.Server, opt)
	if err != nil {
		return nil, err
	}

	natsim.irc = irc.IRC(natsim.Irc.Nick, "natsim")
	natsim.irc.AddCallback("001", natsim.ircJoin)
	natsim.irc.AddCallback("366", func(e *irc.Event) {})
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

		case "subscribeAll":
			for _, subject := range natsim.Nats.Subjects {
				if _, err := natsim.nc.Subscribe(subject, natsim.natsReceive); err != nil {
					natsim.ircSendError("Subscribe", err)
				}
			}

		case "version":
			natsim.ircSend("natsim " + version)

		default:
			natsim.ircSend("Unknown command: " + cmd.name)
		}
	}
}

/**************** IRC Callbacks ****************/

func (natsim *NatsIM) ircJoin(e *irc.Event) {
	natsim.irc.Join(natsim.Irc.Channel)
	natsim.cmdQueue <- command{name: "subscribeAll", arg: ""}
}

func (natsim *NatsIM) ircReceive(e *irc.Event) {
	msg := e.Message()
	if name, arg, found := unpackMark(natsim.Irc.Cmd, msg, true); found {
		natsim.cmdQueue <- command{name: name, arg: arg}
	} else if subject, data, found := unpackMark(natsim.Irc.Send, msg, false); found {
		if err := natsim.nc.Publish(subject, []byte(data)); err != nil {
			natsim.ircSendError("Publish", err)
		}
	}
}

func (natsim *NatsIM) ircSendError(context string, err error) {
	prefix := "[E]"
	if context != "" {
		prefix += context + ": "
	}
	natsim.ircSend(prefix + err.Error())
}

func (natsim *NatsIM) ircSend(s string) {
	if natsim.Irc.MaxLine <= 0 || len(s) < natsim.Irc.MaxLine {
		natsim.ircQueue <- s
	} else {
		for offset := 0; offset < len(s); {
			l := len(s) - offset
			natsim.buf.Reset()
			if offset > 0 {
				natsim.buf.WriteString(natsim.Irc.ContPrefix)
			}

			if natsim.buf.Len()+l <= natsim.Irc.MaxLine {
				natsim.buf.WriteString(s[offset:])
			} else {
				l = natsim.Irc.MaxLine - natsim.buf.Len() - len(natsim.Irc.ContSuffix)
				natsim.buf.WriteString(s[offset : offset+l])
				natsim.buf.WriteString(natsim.Irc.ContSuffix)
			}

			natsim.ircQueue <- natsim.buf.String()
			offset += l
		}
	}
}

func (natsim *NatsIM) ircSender() {
	for {
		line, ok := <-natsim.ircQueue
		if !ok {
			return
		}

		// TODO: rate limitation

		natsim.irc.Privmsg(natsim.Irc.Channel, line)
	}
}

/**************** Nats Callbacks ****************/

func (natsim *NatsIM) natsReceive(m *nats.Msg) {
	msg := packMark(natsim.Irc.Show, m.Subject, string(m.Data))
	natsim.ircSend(msg)
}

/**************** Tools ****************/

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
