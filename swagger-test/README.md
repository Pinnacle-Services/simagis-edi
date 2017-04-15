# swagger-java-client

## Requirements

Building the API client library requires [Maven](https://maven.apache.org/) to be installed.

## Installation

To install the API client library to your local Maven repository, simply execute:

```shell
mvn install
```

To deploy it to a remote Maven repository instead, configure the settings of the repository and execute:

```shell
mvn deploy
```

Refer to the [official documentation](https://maven.apache.org/plugins/maven-deploy-plugin/usage.html) for more information.

### Maven users

Add this dependency to your project's POM:

```xml
<dependency>
    <groupId>io.swagger</groupId>
    <artifactId>swagger-java-client</artifactId>
    <version>1.0.0</version>
    <scope>compile</scope>
</dependency>
```

### Gradle users

Add this dependency to your project's build file:

```groovy
compile "io.swagger:swagger-java-client:1.0.0"
```

### Others

At first generate the JAR by executing:

    mvn package

Then manually install the following JARs:

* target/swagger-java-client-1.0.0.jar
* target/lib/*.jar

## Getting Started

Please follow the [installation](#installation) instruction and execute the following Java code:

```java

import io.swagger.client.*;
import io.swagger.client.auth.*;
import io.swagger.client.model.*;
import io.swagger.client.api.TestApi;

import java.io.File;
import java.util.*;

public class TestApiExample {

    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        
        // Configure API key authorization: Bearer
        ApiKeyAuth Bearer = (ApiKeyAuth) defaultClient.getAuthentication("Bearer");
        Bearer.setApiKey("YOUR API KEY");
        // Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
        //Bearer.setApiKeyPrefix("Token");

        TestApi apiInstance = new TestApi();
        InputParameters webServiceParameters = new InputParameters(); // InputParameters | Input parameters to the web service.
        try {
            WebServiceResult result = apiInstance.add(webServiceParameters);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling TestApi#add");
            e.printStackTrace();
        }
    }
}

```

## Documentation for API Endpoints

All URIs are relative to *http://13.82.108.93:12800*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*TestApi* | [**add**](docs/TestApi.md#add) | **POST** /api/test/v1.0.0 | 
*TestApi* | [**cancelAndDeleteBatchExecution**](docs/TestApi.md#cancelAndDeleteBatchExecution) | **DELETE** /api/test/v1.0.0/batch/{executionId} | Cancels and deletes all batch executions for test.
*TestApi* | [**getBatchExecutionFile**](docs/TestApi.md#getBatchExecutionFile) | **GET** /api/test/v1.0.0/batch/{executionId}/{index}/files/{fileName} | Gets a specific file from an execution in test.
*TestApi* | [**getBatchExecutionFiles**](docs/TestApi.md#getBatchExecutionFiles) | **GET** /api/test/v1.0.0/batch/{executionId}/{index}/files | Gets all files from an individual execution in test.
*TestApi* | [**getBatchExecutionStatus**](docs/TestApi.md#getBatchExecutionStatus) | **GET** /api/test/v1.0.0/batch/{executionId} | Gets all batch executions for test.
*TestApi* | [**getBatchExecutions**](docs/TestApi.md#getBatchExecutions) | **GET** /api/test/v1.0.0/batch | Gets all batch executions for test.
*TestApi* | [**startBatchExecution**](docs/TestApi.md#startBatchExecution) | **POST** /api/test/v1.0.0/batch | 
*UserApi* | [**login**](docs/UserApi.md#login) | **POST** /login | Logs the user in
*UserApi* | [**renewToken**](docs/UserApi.md#renewToken) | **POST** /login/refreshToken | The user renews access token and refresh token
*UserApi* | [**revokeRefreshToken**](docs/UserApi.md#revokeRefreshToken) | **DELETE** /login/refreshToken/{refreshToken} | The user revokes a refresh token


## Documentation for Models

 - [AccessTokenResponse](docs/AccessTokenResponse.md)
 - [BatchWebServiceResult](docs/BatchWebServiceResult.md)
 - [Error](docs/Error.md)
 - [InputParameters](docs/InputParameters.md)
 - [LoginRequest](docs/LoginRequest.md)
 - [OutputParameters](docs/OutputParameters.md)
 - [RenewTokenRequest](docs/RenewTokenRequest.md)
 - [StartBatchExecutionResponse](docs/StartBatchExecutionResponse.md)
 - [WebServiceResult](docs/WebServiceResult.md)


## Documentation for Authorization

Authentication schemes defined for the API:
### Bearer

- **Type**: API key
- **API key parameter name**: Authorization
- **Location**: HTTP header


## Recommendation

It's recommended to create an instance of `ApiClient` per thread in a multithreaded environment to avoid any potential issues.

## Author



