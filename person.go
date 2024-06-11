package claim_conductor

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)

type Person struct {
	PersonID  string `dynamodbav:"person_id" json:"person_id"`
	Name      string `dynamodbav:"name" json:"name"`
	Timestamp string `dynamodbav:"timestamp" json:"timestamp"`
}

type DynamoPerson struct {
	PK string `dynamodbav:"PK"`
	Person
}

type DynamoPersonHistory struct {
	PK         string `dynamodbav:"PK"`
	SK         string `dynamodbav:"SK"`
	UpdateType string `dynamodbav:"update_type"`
	Person
}

func (p Person) SerializePerson() DynamoPerson {
	return DynamoPerson{
		PK:     p.PersonID,
		Person: p,
	}
}

func (p Person) SerializePersonHistory(UpdateType string) DynamoPersonHistory {
	return DynamoPersonHistory{
		PK:         p.PersonID,
		SK:         fmt.Sprintf("%s#%s", p.Timestamp, uuid.New().String()), // using uuid in SK to cover events with similar timestamp
		UpdateType: UpdateType,
		Person:     p,
	}
}

type PersonRepo struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

// returns latest snapshot of given personID
func (repo PersonRepo) GetPerson(personID string) (DynamoPerson, error) {
	p := DynamoPerson{}
	response, err := repo.DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: personID},
		}, TableName: aws.String(repo.TableName),
	})
	if err != nil {
		fmt.Printf("Couldn't get info about %s. Here's why: %v\n", personID, err)
		return DynamoPerson{}, err
	} else {
		err = attributevalue.UnmarshalMap(response.Item, &p)
		if err != nil {
			fmt.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
			return DynamoPerson{}, err
		}
	}
	return p, err
}

type PersonHistoryRepo struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

func (repo PersonHistoryRepo) GetPersonHistory(personID string) []DynamoPersonHistory {
	p := []DynamoPersonHistory{}
	//todo use Query with index on PersonHistory to return history of updates on a person item
	return p
}
