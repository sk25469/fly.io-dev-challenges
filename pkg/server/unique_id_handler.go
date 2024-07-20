package server

import (
	"github.com/google/uuid"
)

func GenerateUniqueId() string {
	id := uuid.New()
	// fmt.Printf("id: %v", id)
	return id.String()

}
