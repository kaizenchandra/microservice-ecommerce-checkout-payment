package com.synechisveltiosi.cartservice.infrastructure;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;

import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Configuration
public class JwtConfiguration {
    static JwtAuthenticationConverter authenticationConverter() {
        var roles = new JwtGrantedAuthoritiesConverter();
        roles.setAuthoritiesClaimName("roles");
        roles.setAuthorityPrefix("ROLE_");
        var converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(roles);
        return converter;
    }

    @Bean
    JwtDecoder jwtDecoder(@Value("${security.jwt.secret}") String secret,
                          @Value("${security.jwt.issuer:checkout-demo}") String issuer,
                          @Value("${security.jwt.audience:ecommerce-api}") String audience) {
        if (secret.getBytes(StandardCharsets.UTF_8).length < 32)
            throw new IllegalArgumentException("JWT signing key must contain at least 32 bytes");
        var decoder = NimbusJwtDecoder.withSecretKey(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
                .macAlgorithm(MacAlgorithm.HS256).build();
        OAuth2TokenValidator<Jwt> claims = jwt -> {
            try {
                boolean valid = jwt.getAudience() != null && jwt.getAudience().contains(audience) && jwt.getExpiresAt() != null && jwt.getIssuedAt() != null
                        && jwt.getIssuedAt().isBefore(Instant.now().plusSeconds(30))
                        && jwt.getExpiresAt().isAfter(jwt.getIssuedAt())
                        && !jwt.getExpiresAt().isAfter(jwt.getIssuedAt().plusSeconds(3600));
                Object roles = jwt.getClaims().get("roles");
                if (!(roles instanceof List<?> list) || list.size() != 1) valid = false;
                else {
                    Object role = list.getFirst();
                    String subject = jwt.getSubject();
                    if ("CUSTOMER".equals(role)) {
                        try {
                            valid &= UUID.fromString(subject).toString().equals(subject);
                        } catch (RuntimeException error) {
                            valid = false;
                        }
                    } else if ("ADMIN".equals(role)) valid &= "admin".equals(subject);
                    else if ("CHECKOUT".equals(role)) valid &= "checkout".equals(subject);
                    else valid = false;
                }
                return valid ? OAuth2TokenValidatorResult.success() : OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token", "Invalid token claims", null));
            } catch (RuntimeException error) {
                return OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token", "Invalid token claims", null));
            }
        };
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(new JwtTimestampValidator(Duration.ofSeconds(30)), new JwtIssuerValidator(issuer), claims));
        return decoder;
    }
}
