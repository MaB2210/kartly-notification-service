package com.kartly.notification_service.eos;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/admin/eos-demo")
public class ExactlyOnceDemoController {

    private final ExactlyOnceDemoService service;

    public ExactlyOnceDemoController(ExactlyOnceDemoService service) {
        this.service = service;
    }

    @PostMapping
    public Map<String, Object> run(@RequestParam(defaultValue = "false") boolean fail) {
        return service.run(fail);
    }
}