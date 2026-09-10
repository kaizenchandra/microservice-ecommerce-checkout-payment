package com.synechisveltiosi.checkoutservice.infrastructure;
import org.springframework.context.annotation.*;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.*;
@Configuration
@org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication(type = org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication.Type.SERVLET)
public class SecurityConfiguration {
    private AuthenticationEntryPoint problemAuthenticationEntryPoint() {
        return (request, response, failure) -> { response.setStatus(401); response.setHeader("WWW-Authenticate", "Bearer");
            response.setContentType("application/problem+json"); response.getWriter().write("{\"status\":401,\"detail\":\"Valid authentication is required\"}"); };
    }
    @Bean
    SecurityFilterChain security(HttpSecurity http) throws Exception {
        return http.csrf(csrf -> csrf.disable()).sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth.requestMatchers("/actuator/health/**", "/actuator/prometheus").permitAll()
                .requestMatchers("/actuator/**").hasRole("ADMIN").requestMatchers("/api/**").authenticated().anyRequest().denyAll())
            .oauth2ResourceServer(oauth -> oauth.jwt(jwt -> jwt.jwtAuthenticationConverter(JwtConfiguration.authenticationConverter()))
                    .authenticationEntryPoint(problemAuthenticationEntryPoint()))
            .exceptionHandling(errors -> errors.authenticationEntryPoint(problemAuthenticationEntryPoint())).build();
    }
}
