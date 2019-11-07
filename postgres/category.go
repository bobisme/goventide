package postgres

import "context"

const categortyStmt = "SELECT * FROM category($1::varchar)"

func (g *get) Category(ctx context.Context, streamName string) (string, error) {
	var c string
	if err := g.db.QueryRow(ctx, categortyStmt, streamName).Scan(&c); err != nil {
		return "", err
	}
	return c, nil
}
