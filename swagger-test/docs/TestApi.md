# TestApi

All URIs are relative to *http://13.82.108.93:12800*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add**](TestApi.md#add) | **POST** /api/test/v1.0.0 | 
[**cancelAndDeleteBatchExecution**](TestApi.md#cancelAndDeleteBatchExecution) | **DELETE** /api/test/v1.0.0/batch/{executionId} | Cancels and deletes all batch executions for test.
[**getBatchExecutionFile**](TestApi.md#getBatchExecutionFile) | **GET** /api/test/v1.0.0/batch/{executionId}/{index}/files/{fileName} | Gets a specific file from an execution in test.
[**getBatchExecutionFiles**](TestApi.md#getBatchExecutionFiles) | **GET** /api/test/v1.0.0/batch/{executionId}/{index}/files | Gets all files from an individual execution in test.
[**getBatchExecutionStatus**](TestApi.md#getBatchExecutionStatus) | **GET** /api/test/v1.0.0/batch/{executionId} | Gets all batch executions for test.
[**getBatchExecutions**](TestApi.md#getBatchExecutions) | **GET** /api/test/v1.0.0/batch | Gets all batch executions for test.
[**startBatchExecution**](TestApi.md#startBatchExecution) | **POST** /api/test/v1.0.0/batch | 


<a name="add"></a>
# **add**
> WebServiceResult add(webServiceParameters)



Consume the test web service.

### Example
```java
// Import classes:
//import io.swagger.client.ApiClient;
//import io.swagger.client.ApiException;
//import io.swagger.client.Configuration;
//import io.swagger.client.auth.*;
//import io.swagger.client.api.TestApi;

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
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **webServiceParameters** | [**InputParameters**](InputParameters.md)| Input parameters to the web service. |

### Return type

[**WebServiceResult**](WebServiceResult.md)

### Authorization

[Bearer](../README.md#Bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="cancelAndDeleteBatchExecution"></a>
# **cancelAndDeleteBatchExecution**
> List&lt;String&gt; cancelAndDeleteBatchExecution(executionId)

Cancels and deletes all batch executions for test.

### Example
```java
// Import classes:
//import io.swagger.client.ApiClient;
//import io.swagger.client.ApiException;
//import io.swagger.client.Configuration;
//import io.swagger.client.auth.*;
//import io.swagger.client.api.TestApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure API key authorization: Bearer
ApiKeyAuth Bearer = (ApiKeyAuth) defaultClient.getAuthentication("Bearer");
Bearer.setApiKey("YOUR API KEY");
// Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
//Bearer.setApiKeyPrefix("Token");

TestApi apiInstance = new TestApi();
String executionId = "executionId_example"; // String | Execution id of the execution.
try {
    List<String> result = apiInstance.cancelAndDeleteBatchExecution(executionId);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling TestApi#cancelAndDeleteBatchExecution");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **executionId** | **String**| Execution id of the execution. |

### Return type

**List&lt;String&gt;**

### Authorization

[Bearer](../README.md#Bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="getBatchExecutionFile"></a>
# **getBatchExecutionFile**
> File getBatchExecutionFile(executionId, index, fileName)

Gets a specific file from an execution in test.

### Example
```java
// Import classes:
//import io.swagger.client.ApiClient;
//import io.swagger.client.ApiException;
//import io.swagger.client.Configuration;
//import io.swagger.client.auth.*;
//import io.swagger.client.api.TestApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure API key authorization: Bearer
ApiKeyAuth Bearer = (ApiKeyAuth) defaultClient.getAuthentication("Bearer");
Bearer.setApiKey("YOUR API KEY");
// Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
//Bearer.setApiKeyPrefix("Token");

TestApi apiInstance = new TestApi();
String executionId = "executionId_example"; // String | Execution id of the execution
Integer index = 56; // Integer | Index of the execution in the batch.
String fileName = "fileName_example"; // String | Name of the file to be returned.
try {
    File result = apiInstance.getBatchExecutionFile(executionId, index, fileName);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling TestApi#getBatchExecutionFile");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **executionId** | **String**| Execution id of the execution |
 **index** | **Integer**| Index of the execution in the batch. |
 **fileName** | **String**| Name of the file to be returned. |

### Return type

[**File**](File.md)

### Authorization

[Bearer](../README.md#Bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="getBatchExecutionFiles"></a>
# **getBatchExecutionFiles**
> List&lt;String&gt; getBatchExecutionFiles(executionId, index)

Gets all files from an individual execution in test.

### Example
```java
// Import classes:
//import io.swagger.client.ApiClient;
//import io.swagger.client.ApiException;
//import io.swagger.client.Configuration;
//import io.swagger.client.auth.*;
//import io.swagger.client.api.TestApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure API key authorization: Bearer
ApiKeyAuth Bearer = (ApiKeyAuth) defaultClient.getAuthentication("Bearer");
Bearer.setApiKey("YOUR API KEY");
// Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
//Bearer.setApiKeyPrefix("Token");

TestApi apiInstance = new TestApi();
String executionId = "executionId_example"; // String | Execution id of the execution
Integer index = 56; // Integer | Index of the execution in the batch.
try {
    List<String> result = apiInstance.getBatchExecutionFiles(executionId, index);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling TestApi#getBatchExecutionFiles");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **executionId** | **String**| Execution id of the execution |
 **index** | **Integer**| Index of the execution in the batch. |

### Return type

**List&lt;String&gt;**

### Authorization

[Bearer](../README.md#Bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="getBatchExecutionStatus"></a>
# **getBatchExecutionStatus**
> BatchWebServiceResult getBatchExecutionStatus(executionId, showPartialResults)

Gets all batch executions for test.

### Example
```java
// Import classes:
//import io.swagger.client.ApiClient;
//import io.swagger.client.ApiException;
//import io.swagger.client.Configuration;
//import io.swagger.client.auth.*;
//import io.swagger.client.api.TestApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure API key authorization: Bearer
ApiKeyAuth Bearer = (ApiKeyAuth) defaultClient.getAuthentication("Bearer");
Bearer.setApiKey("YOUR API KEY");
// Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
//Bearer.setApiKeyPrefix("Token");

TestApi apiInstance = new TestApi();
String executionId = "executionId_example"; // String | Execution id of the execution
Boolean showPartialResults = true; // Boolean | Returns the already processed results of the batch execution even if it hasn't been fully completed.
try {
    BatchWebServiceResult result = apiInstance.getBatchExecutionStatus(executionId, showPartialResults);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling TestApi#getBatchExecutionStatus");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **executionId** | **String**| Execution id of the execution |
 **showPartialResults** | **Boolean**| Returns the already processed results of the batch execution even if it hasn&#39;t been fully completed. | [optional]

### Return type

[**BatchWebServiceResult**](BatchWebServiceResult.md)

### Authorization

[Bearer](../README.md#Bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="getBatchExecutions"></a>
# **getBatchExecutions**
> List&lt;String&gt; getBatchExecutions()

Gets all batch executions for test.

### Example
```java
// Import classes:
//import io.swagger.client.ApiClient;
//import io.swagger.client.ApiException;
//import io.swagger.client.Configuration;
//import io.swagger.client.auth.*;
//import io.swagger.client.api.TestApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure API key authorization: Bearer
ApiKeyAuth Bearer = (ApiKeyAuth) defaultClient.getAuthentication("Bearer");
Bearer.setApiKey("YOUR API KEY");
// Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
//Bearer.setApiKeyPrefix("Token");

TestApi apiInstance = new TestApi();
try {
    List<String> result = apiInstance.getBatchExecutions();
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling TestApi#getBatchExecutions");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

**List&lt;String&gt;**

### Authorization

[Bearer](../README.md#Bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="startBatchExecution"></a>
# **startBatchExecution**
> StartBatchExecutionResponse startBatchExecution(batchWebServiceParameters, parallelCount)



Consume the test web service asynchronously.

### Example
```java
// Import classes:
//import io.swagger.client.ApiClient;
//import io.swagger.client.ApiException;
//import io.swagger.client.Configuration;
//import io.swagger.client.auth.*;
//import io.swagger.client.api.TestApi;

ApiClient defaultClient = Configuration.getDefaultApiClient();

// Configure API key authorization: Bearer
ApiKeyAuth Bearer = (ApiKeyAuth) defaultClient.getAuthentication("Bearer");
Bearer.setApiKey("YOUR API KEY");
// Uncomment the following line to set a prefix for the API key, e.g. "Token" (defaults to null)
//Bearer.setApiKeyPrefix("Token");

TestApi apiInstance = new TestApi();
List<InputParameters> batchWebServiceParameters = Arrays.asList(new InputParameters()); // List<InputParameters> | Input parameters to the web service.
Integer parallelCount = 56; // Integer | Number of threads used to process entries in the batch. Default value is 10. Please make sure not to use too high of a number because it might negatively impact performance.
try {
    StartBatchExecutionResponse result = apiInstance.startBatchExecution(batchWebServiceParameters, parallelCount);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling TestApi#startBatchExecution");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **batchWebServiceParameters** | [**List&lt;InputParameters&gt;**](InputParameters.md)| Input parameters to the web service. |
 **parallelCount** | **Integer**| Number of threads used to process entries in the batch. Default value is 10. Please make sure not to use too high of a number because it might negatively impact performance. | [optional]

### Return type

[**StartBatchExecutionResponse**](StartBatchExecutionResponse.md)

### Authorization

[Bearer](../README.md#Bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

