![CI][ci-status]
[![PkgGoDev][pkg-go-dev-badge]][pkg-go-dev]

# go-aws-timestream-driver

```sh
go get github.com/aereal/go-aws-timestream-driver
```

## Usage

```go
import (
  "database/sql"

  "github.com/aereal/go-aws-timestream-driver"
)

func main() {
  sql.Open(timestreamdriver.DriverName, "awstimestream:///")
}
```

See also Data Source Name format section.

## Data Source Name format

In URI template normative definition:

```
awstimestream://{customEndpointHost}/{?region,accessKeyID,secretAccessKey,enableXray}
```

Example:

```
awstimestream://custom-endpoint.example/?region=us-east-1&accessKeyID=my-key&enableXray=true
```

## License

See LICENSE file.

[pkg-go-dev]: https://pkg.go.dev/github.com/aereal/go-aws-timestream-driver
[pkg-go-dev-badge]: https://pkg.go.dev/badge/aereal/go-aws-timestream-driver
[ci-status]: https://github.com/aereal/go-aws-timestream-driver/workflows/CI/badge.svg?branch=main
