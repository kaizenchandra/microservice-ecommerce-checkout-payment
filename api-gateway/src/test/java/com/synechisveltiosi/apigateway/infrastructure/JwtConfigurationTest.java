package com.synechisveltiosi.apigateway.infrastructure;

import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.JwtException;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JwtConfigurationTest {
    static final String SECRET = "test-only-jwt-signing-key-with-32-bytes";
    static final String CUSTOMER = "11111111-1111-1111-1111-111111111111";

    Map<String, Object> claims() {
        long now = Instant.now().getEpochSecond();
        return new HashMap<>(Map.of("sub", CUSTOMER, "iss", "checkout-demo", "aud", List.of("ecommerce-api"), "roles", List.of("CUSTOMER"), "iat", now, "exp", now + 900));
    }

    String encode(Map<String, Object> claims, String algorithm, String key) throws Exception {
        var base64 = Base64.getUrlEncoder().withoutPadding();
        var json = JsonMapper.builder().build();
        String input = base64.encodeToString(json.writeValueAsBytes(Map.of("alg", algorithm, "typ", "JWT"))) + "." + base64.encodeToString(json.writeValueAsBytes(claims));
        var mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return input + "." + base64.encodeToString(mac.doFinal(input.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void validCustomerMapsToItsSubjectAndRole() throws Exception {
        var decoder = new JwtConfiguration().jwtDecoder(SECRET, "checkout-demo", "ecommerce-api");
        var auth = JwtConfiguration.authenticationConverter().convert(decoder.decode(encode(claims(), "HS256", SECRET)));
        assertEquals(CUSTOMER, auth.getName());
        assertTrue(auth.getAuthorities().stream().anyMatch(role -> role.getAuthority().equals("ROLE_CUSTOMER")));
    }

    @Test
    void rejectsMissingInvalidAndOverprivilegedClaims() throws Exception {
        var decoder = new JwtConfiguration().jwtDecoder(SECRET, "checkout-demo", "ecommerce-api");
        for (String name : List.of("aud", "iss", "sub", "roles", "iat", "exp")) {
            var claims = claims();
            claims.remove(name);
            String encoded = encode(claims, "HS256", SECRET);
            assertThrows(JwtException.class, () -> decoder.decode(encoded), name);
        }
        for (var bad : Map.<String, Object>of("aud", List.of("wrong"), "iss", "wrong", "sub", "not-a-uuid", "roles", List.of("CUSTOMER", "ADMIN"),
                "nbf", Instant.now().plusSeconds(120).getEpochSecond(), "exp", Instant.now().plusSeconds(7200).getEpochSecond()).entrySet()) {
            var claims = claims();
            claims.put(bad.getKey(), bad.getValue());
            String encoded = encode(claims, "HS256", SECRET);
            assertThrows(JwtException.class, () -> decoder.decode(encoded), bad.getKey());
        }
    }

    @Test
    void rejectsWrongSignatureAlgorithmAndWeakKey() throws Exception {
        var decoder = new JwtConfiguration().jwtDecoder(SECRET, "checkout-demo", "ecommerce-api");
        for (String algorithm : List.of("none", "HS512")) {
            String encoded = encode(claims(), algorithm, SECRET);
            assertThrows(JwtException.class, () -> decoder.decode(encoded));
        }
        String encoded = encode(claims(), "HS256", SECRET + "wrong");
        assertThrows(JwtException.class, () -> decoder.decode(encoded));
        assertThrows(IllegalArgumentException.class, () -> new JwtConfiguration().jwtDecoder("too-short", "checkout-demo", "ecommerce-api"));
    }
}
