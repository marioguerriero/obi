package main

import (
	"context"
)

type ObiMaster struct {

}

func (m *ObiMaster) ListInfrastructures(ctx context.Context,
		msg *EmptyRequest) (*ListInfrastructuresResponse, error) {
	return nil, nil
}

func (m *ObiMaster) SubmitJob(ctx context.Context,
		jobRequest *SubmitJobRequest) (*EmptyResponse, error) {
	return nil, nil
}