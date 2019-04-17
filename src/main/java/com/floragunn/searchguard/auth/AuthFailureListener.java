package com.floragunn.searchguard.auth;

import java.net.InetAddress;

import com.floragunn.searchguard.user.AuthCredentials;

public interface AuthFailureListener { 
    void onAuthFailure(InetAddress remoteAddress, AuthCredentials authCredentials, Object request);
}
