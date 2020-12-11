package etl

import (
	"context"
)

type Sender interface {
	// Create message and sends it to the first output channel
	Send(ctx context.Context, payload interface{}) error
	// Create message and sends it to the specified output channel
	SendCh(ctx context.Context, outChNr uint, payload interface{}) error
	// Send message to the first output channel
	SendMessage(ctx context.Context, message Message) error
	// Send message to the specified channel
	SendChMessage(ctx context.Context, outChNr uint, message Message) error
}

type senderOnCompleteHook func(ctx context.Context, outMsg Message, outChNr uint) error

type sender struct {
	outputChs      []chan Message
	newMessageOpts []MessageOption
	onCompleteHook senderOnCompleteHook
}

func newSender(outputChs []chan Message, newMessageOpts []MessageOption, onCompleteHook senderOnCompleteHook) sender {
	return sender{
		outputChs:      outputChs,
		newMessageOpts: newMessageOpts,
		onCompleteHook: onCompleteHook,
	}
}

func (s sender) Send(ctx context.Context, payload interface{}) error {
	return s.SendCh(ctx, 0, payload)
}

func (s sender) SendCh(ctx context.Context, channelNr uint, payload interface{}) error {
	msg := NewMessage(payload, s.newMessageOpts...)

	return s.SendChMessage(ctx, channelNr, msg)
}

func (s sender) SendMessage(ctx context.Context, msg Message) error {
	return s.SendCh(ctx, 0, msg)
}

func (s sender) SendChMessage(ctx context.Context, channelNr uint, msg Message) error {
	if len(s.outputChs) < int(channelNr) {
		return ErrOutputMessageOutOfChannelsRange
	}

	if s.onCompleteHook != nil {
		err := s.onCompleteHook(ctx, msg, channelNr)
		if err != nil {
			return err
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.outputChs[channelNr] <- msg:
	}

	return nil
}
