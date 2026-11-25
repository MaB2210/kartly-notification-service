package com.kartly.notification_service.tx;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/admin/tx-demo")
public class TransactionDemoController {

    private final TransactionDemoService service;

    public TransactionDemoController(TransactionDemoService service) {
        this.service = service;
    }

    @PostMapping
    public Map<String, String> run(@RequestParam(defaultValue = "false") boolean abort) {
        return service.run(abort);
    }
}