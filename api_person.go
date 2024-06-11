package claim_conductor

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/awslabs/aws-lambda-go-api-proxy/gin"
	"github.com/gin-gonic/gin"
)

var ginLambda *ginadapter.GinLambda

type Dependencies struct {
	PersonRepo   PersonRepo
	DynamoClient *dynamodb.Client
}

func CreateDependencies(client *dynamodb.Client) Dependencies {
	return Dependencies{
		PersonRepo: PersonRepo{
			DynamoDbClient: client,
			TableName:      "Person",
		},
		DynamoClient: client,
	}
}

type Response struct {
	Message string `json:"message"`
}

type WebhookPayload struct {
	PayloadType    string `json:"payload_type"`
	PayloadContent Person `json:"payload_content"`
}

type GetNameResponse struct {
	Name string `json:"name"`
}

func (dep *Dependencies) addPerson(c *gin.Context, p Person) {
	dynamoPerson := p.SerializePerson()
	personItem, _ := attributevalue.MarshalMap(dynamoPerson)
	dynamoPersonHistory := p.SerializePersonHistory("Add")
	personHistoryItem, _ := attributevalue.MarshalMap(dynamoPersonHistory)
	// using transaction to write to both Person and PersonHistory at same time
	transactItems := []types.TransactWriteItem{
		{
			Put: &types.Put{
				TableName: aws.String("Person"),
				Item:      personItem,
			},
		},
		{
			Put: &types.Put{
				TableName: aws.String("PersonHistory"),
				Item:      personHistoryItem,
			},
		},
	}
	_, err := dep.DynamoClient.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{Message: "Failed to add person"})
		return
	}
	c.JSON(http.StatusOK, Response{Message: "Person added successfully"})
}

func (dep *Dependencies) updatePerson(c *gin.Context, p Person) {
	existingPerson, err := dep.PersonRepo.GetPerson(p.PersonID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{Message: "Internal Error Try Later"})
		return
	}
	if existingPerson == (DynamoPerson{}) {
		c.JSON(http.StatusNotFound, Response{Message: "Not Found"})
		return
	}

	dynamoPerson := p.SerializePerson()
	personItem, _ := attributevalue.MarshalMap(dynamoPerson)
	dynamoPersonHistory := p.SerializePersonHistory("Update")
	personHistoryItem, _ := attributevalue.MarshalMap(dynamoPersonHistory)
	// using transaction to write to both Person and PersonHistory at same time
	transactItems := []types.TransactWriteItem{
		{
			Put: &types.Put{
				TableName: aws.String("Person"),
				Item:      personItem,
			},
		},
		{
			Put: &types.Put{
				TableName: aws.String("PersonHistory"),
				Item:      personHistoryItem,
			},
		},
	}
	_, err = dep.DynamoClient.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{Message: "Failed to update person"})
		return
	}
	c.JSON(http.StatusOK, Response{Message: "Person updated successfully"})
}

func (dep *Dependencies) removePerson(c *gin.Context, p Person) {
	existingPersonDynamo, err := dep.PersonRepo.GetPerson(p.PersonID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{Message: "Internal Error Try Later"})
		return
	}
	if existingPersonDynamo == (DynamoPerson{}) {
		c.JSON(http.StatusNotFound, Response{Message: "Not Found"})
		return
	}
	person := existingPersonDynamo.Person
	dynamoPersonHistory := person.SerializePersonHistory("Remove")
	personHistoryItem, _ := attributevalue.MarshalMap(dynamoPersonHistory)
	// using transaction to write to both Person and PersonHistory at same time
	transactItems := []types.TransactWriteItem{
		{
			Delete: &types.Delete{
				TableName: aws.String("Person"),
				Key: map[string]types.AttributeValue{
					"person_id": &types.AttributeValueMemberS{Value: p.PersonID},
				},
			},
		},
		{
			Put: &types.Put{
				TableName: aws.String("ItemHistory"),
				Item:      personHistoryItem,
			},
		},
	}
	_, err = dep.DynamoClient.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{Message: "Failed to remove person"})
		return
	}
	c.JSON(http.StatusOK, Response{Message: "Person removed successfully"})
}

// gets the latest name of a given person_id
func (dep *Dependencies) getPerson(c *gin.Context) {
	personID := c.Query("person_id")
	if personID == "" {
		c.JSON(http.StatusBadRequest, Response{Message: "Invalid request"})
		return
	}
	dynamoPerson, err := dep.PersonRepo.GetPerson(personID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{Message: "Internal Error Try Later"})
	}
	c.JSON(http.StatusOK, GetNameResponse{Name: dynamoPerson.Name})
}
