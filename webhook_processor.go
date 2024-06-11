package claim_conductor

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ginadapter "github.com/awslabs/aws-lambda-go-api-proxy/gin"
	"github.com/gin-gonic/gin"
)

func init() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	ddbClient := dynamodb.NewFromConfig(cfg)
	deps := CreateDependencies(ddbClient)
	fmt.Printf("Gin cold start")
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	r.POST("/accept_webhook", deps.processWebhook)
	r.GET("/get_name", deps.getPerson)

	ginLambda = ginadapter.New(r)
}

// AWS lambda handler that is fronting and proxies APIGateway Request/Response
func Handler(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// If no name is provided in the HTTP request body, throw an error
	return ginLambda.ProxyWithContext(ctx, req)
}

func (dep *Dependencies) processWebhook(c *gin.Context) {
	var payload WebhookPayload
	if err := c.BindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, Response{Message: "Invalid request"})
		return
	}
	switch payload.PayloadType {
	case "PersonAdded":
		dep.addPerson(c, payload.PayloadContent)
	case "PersonRenamed":
		dep.updatePerson(c, payload.PayloadContent)
	case "PersonRemoved":
		dep.removePerson(c, payload.PayloadContent)
	default:
		c.JSON(http.StatusBadRequest, Response{Message: "Invalid request"})
		return
	}
}

func main() {
	lambda.Start(Handler)
}
