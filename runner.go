package etl

import (
	"context"
	"golang.org/x/sync/errgroup"
)

type Runner interface {
	Run(ctx context.Context) error
}

func RunAll(ctx context.Context, runners ...Runner) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, runner := range runners {
		r := runner
		g.Go(func() error {
			return r.Run(ctx)
		})
	}

	return g.Wait()
}
