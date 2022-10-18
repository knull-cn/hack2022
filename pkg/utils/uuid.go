package utils

import "github.com/google/uuid"

func GenerateClientUUid() string {
	uuid := uuid.New()
	key := uuid.String()
	return key
}
