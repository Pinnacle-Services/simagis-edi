/*
 * test
 * No description provided (generated by Swagger Codegen https://github.com/swagger-api/swagger-codegen)
 *
 * OpenAPI spec version: v1.0.0
 * 
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */


package io.swagger.client.api;

import io.swagger.client.ApiException;
import io.swagger.client.model.AccessTokenResponse;
import io.swagger.client.model.Error;
import io.swagger.client.model.LoginRequest;
import io.swagger.client.model.RenewTokenRequest;
import org.junit.Test;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for UserApi
 */
@Ignore
public class UserApiTest {

    private final UserApi api = new UserApi();

    
    /**
     * Logs the user in
     *
     * 
     *
     * @throws ApiException
     *          if the Api call fails
     */
    @Test
    public void loginTest() throws ApiException {
        LoginRequest loginRequest = new LoginRequest();
        loginRequest.setUsername("admin");
        loginRequest.setPassword("!Khvatkov1");
        AccessTokenResponse response = api.login(loginRequest);


        // TODO: test validations
    }
    
    /**
     * The user renews access token and refresh token
     *
     * 
     *
     * @throws ApiException
     *          if the Api call fails
     */
    @Test
    public void renewTokenTest() throws ApiException {
        RenewTokenRequest renewTokenRequest = null;
        AccessTokenResponse response = api.renewToken(renewTokenRequest);

        // TODO: test validations
    }
    
    /**
     * The user revokes a refresh token
     *
     * 
     *
     * @throws ApiException
     *          if the Api call fails
     */
    @Test
    public void revokeRefreshTokenTest() throws ApiException {
        String refreshToken = null;
        AccessTokenResponse response = api.revokeRefreshToken(refreshToken);

        // TODO: test validations
    }
    
}
