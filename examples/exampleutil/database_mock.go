//go:build examples

package exampleutil

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// MockDatabase is a toy in-memory database for examples.
// It simulates realistic database behavior with latency and context support.
// DO NOT use this in production - it's purely for demonstration purposes.
type MockDatabase struct {
	users    map[string]*User
	products map[string]*Product
	mu       sync.RWMutex
	latency  time.Duration
}

// NewMockDatabase creates a new mock database with sample data.
// This is a toy database for demonstration purposes only.
func NewMockDatabase() *MockDatabase {
	return &MockDatabase{
		users: map[string]*User{
			"1": {ID: "1", Name: "Alice Smith", Email: "alice@example.com", UpdatedAt: time.Now()},
			"2": {ID: "2", Name: "Bob Jones", Email: "bob@example.com", UpdatedAt: time.Now()},
			"3": {ID: "3", Name: "Charlie Brown", Email: "charlie@example.com", UpdatedAt: time.Now()},
		},
		products: map[string]*Product{
			"prod-1": {ID: "prod-1", Name: "Laptop", Description: "High-performance laptop", Price: 1299.99, Stock: 50},
			"prod-2": {ID: "prod-2", Name: "Mouse", Description: "Wireless mouse", Price: 29.99, Stock: 200},
			"prod-3": {ID: "prod-3", Name: "Keyboard", Description: "Mechanical keyboard", Price: 149.99, Stock: 75},
		},
		latency: 10 * time.Millisecond, // Simulate network/disk latency (reduced for tests)
	}
}

// GetUser fetches a user by ID with context support.
func (db *MockDatabase) GetUser(ctx context.Context, id string) (*User, error) {
	// Respect context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(db.latency):
		// Continue with operation
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	user, exists := db.users[id]
	if !exists {
		return nil, sql.ErrNoRows
	}

	// Return a copy to prevent external modification
	userCopy := *user
	return &userCopy, nil
}

// GetUsers fetches multiple users with context support.
func (db *MockDatabase) GetUsers(ctx context.Context, ids []string) (map[string]*User, error) {
	// Respect context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(db.latency):
		// Continue with operation
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	users := make(map[string]*User)
	for _, id := range ids {
		if user, exists := db.users[id]; exists {
			userCopy := *user
			users[id] = &userCopy
		}
	}

	return users, nil
}

// UpdateUser updates a user with context support.
func (db *MockDatabase) UpdateUser(ctx context.Context, user *User) error {
	// Check context before expensive operation
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Simulate write latency (typically longer than reads)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(db.latency * 2):
		// Continue with operation
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.users[user.ID]; !exists {
		return fmt.Errorf("user %s not found", user.ID)
	}

	user.UpdatedAt = time.Now()
	db.users[user.ID] = user
	return nil
}

// GetProduct fetches a product with context support.
func (db *MockDatabase) GetProduct(ctx context.Context, id string) (*Product, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(db.latency):
		// Continue
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	product, exists := db.products[id]
	if !exists {
		return nil, sql.ErrNoRows
	}

	productCopy := *product
	return &productCopy, nil
}

// UpdateProductStock updates product stock with optimistic locking.
func (db *MockDatabase) UpdateProductStock(ctx context.Context, productID string, delta int) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(db.latency * 2):
		// Continue
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	product, exists := db.products[productID]
	if !exists {
		return fmt.Errorf("product %s not found", productID)
	}

	newStock := product.Stock + delta
	if newStock < 0 {
		return fmt.Errorf("insufficient stock")
	}

	product.Stock = newStock
	return nil
}
