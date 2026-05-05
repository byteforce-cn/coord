package cn.byteforce.mock.inventory.controller;

import cn.byteforce.mock.inventory.config.CoordAuthTokenHolder;
import cn.byteforce.mock.inventory.service.InventoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class InventoryController {

    @Autowired private InventoryService inventoryService;
    @Autowired private CoordAuthTokenHolder authTokenHolder;

    @GetMapping("/inventory/{productId}")
    public ResponseEntity<Map<String, Object>> getStock(@PathVariable String productId) {
        return ResponseEntity.ok(Map.of(
                "productId", productId,
                "stock", inventoryService.getStock(productId)
        ));
    }

    @PostMapping("/inventory/{productId}/deduct")
    public ResponseEntity<?> deduct(@PathVariable String productId,
                                     @RequestBody Map<String, Object> body) {
        int quantity = Integer.parseInt(String.valueOf(body.get("quantity")));
        boolean ok = inventoryService.deduct(productId, quantity);
        if (ok) {
            return ResponseEntity.ok(Map.of("success", true,
                    "remaining", inventoryService.getStock(productId)));
        }
        return ResponseEntity.status(409).body(Map.of("success", false, "reason", "库存不足"));
    }

    @PostMapping("/inventory/{productId}/rollback")
    public ResponseEntity<?> rollback(@PathVariable String productId,
                                       @RequestBody Map<String, Object> body) {
        int quantity = Integer.parseInt(String.valueOf(body.get("quantity")));
        inventoryService.rollback(productId, quantity);
        return ResponseEntity.ok(Map.of("success", true,
                "stock", inventoryService.getStock(productId)));
    }

    /** 初始化库存（测试用） */
    @PutMapping("/inventory/{productId}/init")
    public ResponseEntity<?> initStock(@PathVariable String productId,
                                        @RequestBody Map<String, Object> body) {
        int quantity = Integer.parseInt(String.valueOf(body.get("quantity")));
        inventoryService.initStock(productId, quantity);
        return ResponseEntity.ok(Map.of("productId", productId, "stock", quantity));
    }

    @GetMapping("/inventory")
    public ResponseEntity<?> listAll() {
        return ResponseEntity.ok(inventoryService.allStock());
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "inventory-service"));
    }

    @PostMapping("/internal/set-coord-token")
    public ResponseEntity<Map<String, String>> setCoordToken(@RequestBody Map<String, String> body) {
        String token = body.get("token");
        authTokenHolder.setToken(token);
        return ResponseEntity.ok(Map.of("status", "ok"));
    }
}
