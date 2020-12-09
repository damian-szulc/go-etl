package etl

import "context"

type TransformerSender interface {
	// Sends message to the first output channel
	Send(ctx context.Context, payload interface{}) error
	// Sends message to the specific output channel
	SendCh(ctx context.Context, outChNr uint, payload interface{}) error
}

type transformerSender struct {
	inMsg     Message
	outputChs []chan Message

	hooksOnComplete []TransformerOnComplete
}

func newTransformerSender(inMsg Message, outputChs []chan Message, hooksOnComplete []TransformerOnComplete) transformerSender {
	return transformerSender{
		inMsg:           inMsg,
		outputChs:       outputChs,
		hooksOnComplete: hooksOnComplete,
	}
}

func (s transformerSender) onCompleteHook(ctx context.Context, inMsg Message, outMsg Message, channelNr uint) error {
	var err error
	for _, hook := range s.hooksOnComplete {
		err = hook(ctx, inMsg, outMsg, channelNr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s transformerSender) Send(ctx context.Context, payload interface{}) error {
	return s.SendCh(ctx, 0, payload)
}

func (s transformerSender) SendCh(ctx context.Context, channelNr uint, payload interface{}) error {
	if len(s.outputChs) > int(channelNr) {
		return ErrOutputMessageOutOfChannelsRange
	}

	msg := NewMessage(payload, MessageWithProcessingStartedAt(s.inMsg.ProcessingStartedAt()))

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.outputChs[channelNr] <- msg:
	}

	return s.onCompleteHook(ctx, s.inMsg, msg, channelNr)
}
