package api_test

import (
	"encoding/json"
	"errors"
	"github.com/fishstick/internal/api"
	"reflect"
	"testing"

	"github.com/go-playground/validator/v10"
)

type validationRequest struct {
	Email string `json:"email_address,omitempty" validate:"required,email"`
	Age   int    `json:"age" validate:"gte=18"`
}

func TestValidateValid(t *testing.T) {
	t.Parallel()
	request := validationRequest{Email: "person@example.com", Age: 18}
	for _, value := range []any{request, &request} {
		if err := api.Validate(value); err != nil {
			t.Fatal(err)
		}
	}
}

func TestValidateFieldErrors(t *testing.T) {
	t.Parallel()
	err := api.Validate(validationRequest{Email: "invalid", Age: 17})
	var fields api.ValidationErrors
	if !errors.As(err, &fields) {
		t.Fatalf("expected api.ValidationErrors, got %v", err)
	}
	want := api.ValidationErrors{
		{Field: "email_address", Rule: "email"},
		{Field: "age", Rule: "gte", Param: "18"},
	}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("fields = %#v, want %#v", fields, want)
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `[{"field":"email_address","rule":"email"},{"field":"age","rule":"gte","param":"18"}]` {
		t.Fatalf("unexpected JSON: %s", encoded)
	}
}

func TestValidateNestedJSONPaths(t *testing.T) {
	t.Parallel()
	type item struct {
		Name string `json:"display_name" validate:"required"`
	}
	request := struct {
		Items []item `json:"items" validate:"dive"`
	}{Items: []item{{}}}
	var fields api.ValidationErrors
	if err := api.Validate(request); !errors.As(err, &fields) {
		t.Fatalf("expected api.ValidationErrors, got %v", err)
	}
	if len(fields) != 1 || fields[0].Field != "items[0].display_name" {
		t.Fatalf("unexpected nested fields: %#v", fields)
	}
}

func TestValidateInvalidInput(t *testing.T) {
	t.Parallel()
	for _, value := range []any{nil, (*validationRequest)(nil), "not a struct"} {
		err := api.Validate(value)
		var invalid *validator.InvalidValidationError
		if !errors.As(err, &invalid) {
			t.Fatalf("expected InvalidValidationError for %T, got %v", value, err)
		}
		var fields api.ValidationErrors
		if errors.As(err, &fields) {
			t.Fatalf("programming error misclassified as request error: %v", err)
		}
	}
}
