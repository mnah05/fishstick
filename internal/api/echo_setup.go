package api

import (
	"fmt"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type echoValidator struct{}

func (echoValidator) Validate(i any) error {
	return Validate(i)
}

// newEcho installs the shared base middleware; test mode stops at recover so
// tests do not log or dump request bodies.
func newEcho(testMode bool) *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.Validator = echoValidator{}
	e.Use(middleware.Recover())
	if testMode {
		return e
	}
	e.Use(middleware.CORS())
	e.Use(middleware.RequestLogger())
	e.Use(middleware.BodyDump(func(c echo.Context, reqBody []byte, resBody []byte) {
		fmt.Println("Request Body:", string(reqBody))
		fmt.Println("Response Body:", string(resBody))
	}))
	e.Use(middleware.RequestID())
	e.Use(middleware.Secure())
	e.Use(middleware.BodyLimit("10M"))
	return e
}
