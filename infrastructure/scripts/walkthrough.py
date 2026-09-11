#!/usr/bin/env python3
"""Exercise synthetic order workflows through the gateway; retains the created demo records."""
import argparse
import json
import os
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request
import uuid
from demo_token import IDENTITIES, token

SCENARIOS = {
    "success": (1, "tok_success", "US", "COMPLETED", "RESERVED", "COMPLETED", "NONE"),
    "inventory-rejection": (0, "tok_success", "US", "CANCELLED", "REJECTED", "UNKNOWN", "NONE"),
    "payment-decline": (1, "tok_declined", "US", "CANCELLED", "RELEASED", "FAILED", "NONE"),
    "shipment-refund": (1, "tok_success", "ZZ", "CANCELLED", "RELEASED", "COMPLETED", "REFUNDED"),
}


class Walkthrough:
    def __init__(self, gateway, values, timeout):
        self.gateway = gateway.rstrip("/")
        self.values = values
        self.timeout = timeout
        self.trace_id = uuid.uuid4().hex
        self.traceparent = "00-" + self.trace_id + "-" + uuid.uuid4().hex[:16] + "-01"
        self.correlation = str(uuid.uuid4())

    def request(self, method, path, identity="customer", body=None, expected=(200,), key=None):
        headers = {"Content-Type": "application/json", "X-Correlation-ID": self.correlation, "traceparent": self.traceparent}
        if identity:
            headers["Authorization"] = "Bearer " + token(IDENTITIES[identity][0], self.values["JWT_SECRET"],
                                                        issuer=self.values.get("JWT_ISSUER", "checkout-demo"),
                                                        audience=self.values.get("JWT_AUDIENCE", "ecommerce-api"))
        if key:
            headers["Idempotency-Key"] = key
        request = urllib.request.Request(self.gateway + path, method=method, headers=headers,
                                         data=None if body is None else json.dumps(body).encode())
        try:
            response = urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            if response.status not in expected:
                raise RuntimeError(f"{method} {path}: expected {expected}, received {response.status}")
            return response.status, json.loads(response.read())

    def run(self, scenario):
        stock, payment_token, country, outcome, inventory, payment, refund = SCENARIOS[scenario]
        _, product = self.request("POST", "/api/products", "admin", {
            "sku": "WALK-" + uuid.uuid4().hex.upper(), "name": "Synthetic walkthrough item", "description": scenario,
            "price": {"amount": "12.50", "currency": "USD"}}, (201,))
        self.request("POST", "/api/inventory/stock", "admin", {"productId": product["id"], "onHand": stock}, (201,))
        _, cart = self.request("POST", "/api/carts", expected=(201,))
        _, cart = self.request("POST", "/api/carts/" + cart["id"] + "/items", body={
            "productId": product["id"], "quantity": 1, "expectedVersion": cart["version"]})
        # Explicit manual handoff: the checkout HTTP service is not implemented.
        _, authoritative = self.request("GET", "/api/products/" + product["id"])
        order_id, key = str(uuid.uuid4()), str(uuid.uuid4())
        command = {"orderId": order_id, "customerId": IDENTITIES["customer"][0], "cartId": cart["id"], "cartVersion": cart["version"],
                   "items": [{"productId": product["id"], "sku": authoritative["sku"], "name": authoritative["name"], "quantity": 1,
                              "unitPrice": authoritative["price"]}], "paymentToken": payment_token,
                   "shippingAddress": {"recipient": "Demo Buyer", "line1": "1 Test Street", "city": "Test City", "postalCode": "12345", "country": country}}
        _, accepted = self.request("POST", "/api/orders", "checkout", command, (202,), key)
        _, repeated = self.request("POST", "/api/orders", "checkout", command, (202,), key)
        if accepted != repeated:
            raise RuntimeError("Idempotent submission returned a different acceptance")
        self.request("GET", "/api/order-views/" + order_id, "other", expected=(404,))
        deadline = time.monotonic() + self.timeout
        while True:
            status, view = self.request("GET", "/api/order-views/" + order_id, expected=(200, 404))
            if status == 200 and view.get("status") == outcome and view.get("notified") and (
                    view.get("inventoryStatus"), view.get("paymentStatus"), view.get("refundStatus")) == (inventory, payment, refund):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Timed out waiting for {scenario}, order {order_id}; inspect the order view and service logs")
            time.sleep(0.2)
        self.request("GET", "/api/order-views/" + order_id, "other", expected=(404,))
        _, details = self.request("GET", "/api/order-views/" + order_id + "/details")
        if details["inventory"]["availability"] != "AVAILABLE":
            raise RuntimeError("Inventory owner details unavailable")
        for owner in ("payment", "shipping"):
            expected = "NOT_FOUND" if scenario == "inventory-rejection" or (scenario == "payment-decline" and owner == "shipping") else "AVAILABLE"
            if details[owner]["availability"] != expected:
                raise RuntimeError(f"Unexpected {owner} availability for {scenario}")
        return {"scenario": scenario, "orderId": order_id, "productId": product["id"], "cartId": cart["id"],
                "status": view["status"], "inventoryStatus": inventory, "paymentStatus": payment, "refundStatus": refund,
                "notified": view["notified"], "traceId": self.trace_id, "correlationId": self.correlation}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=[*SCENARIOS, "all"], default="success")
    parser.add_argument("--gateway", help="Gateway URL; defaults to localhost and GATEWAY_PORT")
    parser.add_argument("--timeout", type=int, default=90, help="Maximum polling duration per scenario in seconds (1–300)")
    args = parser.parse_args()
    if not 1 <= args.timeout <= 300:
        parser.error("--timeout must be 1–300")
    path = Path(__file__).resolve().parents[2] / ".env"
    values = dict(line.split("=", 1) for line in path.read_text().splitlines() if line and not line.startswith("#") and "=" in line) if path.exists() else {}
    values.update(os.environ)
    try:
        # Validate credentials before creating any fixtures.
        token(IDENTITIES["customer"][0], values.get("JWT_SECRET", ""))
        gateway = args.gateway or "http://localhost:" + values.get("GATEWAY_PORT", "8080")
        for scenario in SCENARIOS if args.scenario == "all" else [args.scenario]:
            print(json.dumps(Walkthrough(gateway, values, args.timeout).run(scenario)), flush=True)
    except (ValueError, RuntimeError, urllib.error.URLError) as error:
        print("Walkthrough failed: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
