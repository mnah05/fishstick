package api

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
)

// FieldError describes a failed rule without exposing the submitted value.
type FieldError struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`
	Param string `json:"param,omitempty"`
}

// ValidationErrors contains request field errors suitable for a JSON response.
type ValidationErrors []FieldError

func (e ValidationErrors) Error() string {
	return fmt.Sprintf("validation failed for %d field(s)", len(e))
}

// Configure once before concurrent requests; validator caches struct metadata.
var requestValidator = func() *validator.Validate {
	v := validator.New(validator.WithRequiredStructEnabled())
	v.RegisterTagNameFunc(func(field reflect.StructField) string {
		name := strings.SplitN(field.Tag.Get("json"), ",", 2)[0]
		if name == "" || name == "-" {
			return field.Name
		}
		return name
	})
	if err := v.RegisterValidation("notblank", func(fl validator.FieldLevel) bool {
		switch value := fl.Field().Interface().(type) {
		case string:
			return strings.TrimSpace(value) != ""
		case *string:
			if value == nil {
				return true
			}
			return strings.TrimSpace(*value) != ""
		default:
			return false
		}
	}); err != nil {
		panic(err)
	}
	return v
}()

// Validate checks validate tags on a struct or non-nil struct pointer.
// Use errors.As to distinguish ValidationErrors (bad request data) from an
// invalid argument (a programming error). Nested fields retain their JSON paths.
func Validate(value any) error {
	err := requestValidator.Struct(value)
	if err == nil {
		return nil
	}
	var failures validator.ValidationErrors
	if !errors.As(err, &failures) {
		return fmt.Errorf("validate request: %w", err)
	}
	typ := reflect.TypeOf(value)
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	prefix := typ.Name() + "."
	fields := make(ValidationErrors, 0, len(failures))
	for _, failure := range failures {
		fields = append(fields, FieldError{
			Field: strings.TrimPrefix(failure.Namespace(), prefix),
			Rule:  failure.Tag(),
			Param: failure.Param(),
		})
	}
	return fields
}
