package com.synechisveltiosi.notificationservice.api;
import com.synechisveltiosi.notificationservice.application.NotificationTransactions;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;
@RestController
@RequestMapping("/api/notification")
public class NotificationController {
    private final NotificationTransactions notifications;
    public NotificationController(NotificationTransactions notifications) { this.notifications = notifications; }
    @GetMapping("/{orderId}") public NotificationTransactions.Delivered get(@PathVariable UUID orderId) { return notifications.get(orderId); }
}
