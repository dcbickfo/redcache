//go:build examples

package exampleutil

import (
	"encoding/json"
	"time"
)

// User represents a user domain object.
type User struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Product represents a product domain object.
type Product struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Price       float64 `json:"price"`
	Stock       int     `json:"stock"`
}

// SerializeUser converts a user to JSON string.
func SerializeUser(user *User) (string, error) {
	data, err := json.Marshal(user)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// DeserializeUser converts JSON string to user.
func DeserializeUser(data string) (*User, error) {
	var user User
	if err := json.Unmarshal([]byte(data), &user); err != nil {
		return nil, err
	}
	return &user, nil
}

// SerializeProduct converts a product to JSON string.
func SerializeProduct(product *Product) (string, error) {
	data, err := json.Marshal(product)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// DeserializeProduct converts JSON string to product.
func DeserializeProduct(data string) (*Product, error) {
	var product Product
	if err := json.Unmarshal([]byte(data), &product); err != nil {
		return nil, err
	}
	return &product, nil
}
