import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    DynamoDBDocumentClient,
    QueryCommand,
    QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
    try {
        console.log("Event: ", JSON.stringify(event));
        const queryParams = event.queryStringParameters;
        if (!queryParams || !queryParams.movieId) {
            return {
                statusCode: 400,
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ message: "Missing query parameter: movieId" }),
            };
        }

        const movieId = parseInt(queryParams.movieId);
        let commandInput: QueryCommandInput = {
            TableName: process.env.CAST_TABLE_NAME,
        };

        if ("roleName" in queryParams) {
            commandInput = {
                ...commandInput,
                IndexName: "roleIx",
                KeyConditionExpression: "movieId = :m and begins_with(roleName, :r) ",
                ExpressionAttributeValues: {
                    ":m": movieId,
                    ":r": queryParams.roleName,
                },
            };
        } else if ("actorName" in queryParams) {
            commandInput = {
                ...commandInput,
                KeyConditionExpression: "movieId = :m and begins_with(actorName, :a) ",
                ExpressionAttributeValues: {
                    ":m": movieId,
                    ":a": queryParams.actorName,
                },
            };
        } else {
            commandInput = {
                ...commandInput,
                KeyConditionExpression: "movieId = :m",
                ExpressionAttributeValues: {
                    ":m": movieId,
                },
            };
        }

        const commandOutput = await ddbDocClient.send(new QueryCommand(commandInput));

        // Fetch movie details
        const movieCommand: QueryCommandInput = {
            TableName: "MoviesTable",
            KeyConditionExpression: "movieId = :movieId",
            ExpressionAttributeValues: {
                ":movieId": movieId,
            },
        };
        const movieResponse = await ddbDocClient.send(new QueryCommand(movieCommand));
        if (!movieResponse.Items || movieResponse.Items.length === 0) {
            return {
                statusCode: 404,
                body: JSON.stringify({ message: "Movie not found" }),
            };
        }
        const movieDetails = movieResponse.Items[0];

        return {
            statusCode: 200,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({
                movieId: movieDetails.movieId,
                title: movieDetails.title,
                genreIds: movieDetails.genreIds,
                overview: movieDetails.overview,
                cast: commandOutput.Items,
            }),
        };
    } catch (error: any) {
        console.log(JSON.stringify(error));
        return {
            statusCode: 500,
            headers: {
                "content-type": "application/json"
            },
            body: JSON.stringify({ error }),
        };
    }
};

function createDocumentClient() {
    const ddbClient = new DynamoDBClient({ region: process.env.REGION });
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = { marshallOptions, unmarshallOptions };
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}