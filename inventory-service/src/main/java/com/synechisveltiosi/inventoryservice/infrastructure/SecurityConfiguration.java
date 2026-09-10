package com.synechisveltiosi.inventoryservice.infrastructure;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Curl-only demo authentication; JWT replaces this adapter in Phase 11.
 */
@Configuration
public class SecurityConfiguration {
    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    UserDetailsService users(PasswordEncoder encoder,
                             @Value("${demo.auth.customer-password}") String customer,
                             @Value("${demo.auth.second-customer-password}") String second,
                             @Value("${demo.auth.admin-password}") String admin) {
        return new InMemoryUserDetailsManager(
                User.withUsername("11111111-1111-1111-1111-111111111111").password(encoder.encode(customer)).roles("CUSTOMER").build(),
                User.withUsername("22222222-2222-2222-2222-222222222222").password(encoder.encode(second)).roles("CUSTOMER").build(),
                User.withUsername("admin").password(encoder.encode(admin)).roles("ADMIN").build());
    }

    private AuthenticationEntryPoint problemAuthenticationEntryPoint() {
        return (request, response, failure) -> {
            response.setStatus(401);
            response.setHeader("WWW-Authenticate", "Basic realm=\"checkout-demo\"");
            response.setContentType("application/problem+json");
            response.getWriter().write("{\"type\":\"about:blank\",\"title\":\"Unauthorized\",\"status\":401,\"detail\":\"Valid authentication is required\"}");
        };
    }

    @Bean
    SecurityFilterChain security(HttpSecurity http) throws Exception {
        return http.csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health/**", "/actuator/prometheus").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/inventory/reservations/*/details").hasAnyRole("CUSTOMER", "ADMIN")
                        .requestMatchers("/api/inventory", "/api/inventory/**").hasRole("ADMIN")
                        .requestMatchers("/actuator/**").hasRole("ADMIN")
                        .anyRequest().denyAll())
                .httpBasic(basic -> basic.authenticationEntryPoint(problemAuthenticationEntryPoint()))
                .exceptionHandling(errors -> errors
                        .authenticationEntryPoint(problemAuthenticationEntryPoint())
                        .accessDeniedHandler((request, response, failure) -> {
                            response.setStatus(403);
                            response.setContentType("application/problem+json");
                            response.getWriter().write("{\"type\":\"about:blank\",\"title\":\"Forbidden\",\"status\":403,\"detail\":\"Insufficient permissions\"}");
                        }))
                .build();
    }
}
