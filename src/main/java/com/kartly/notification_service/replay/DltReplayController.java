package com.kartly.notification_service.replay;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/admin/dlt")
public class DltReplayController {

    private final DltReplayService replayService;

    public DltReplayController(DltReplayService replayService) {
        this.replayService = replayService;
    }

    @PostMapping("/replay")
    public Map<String, Integer> replay(@RequestParam(defaultValue = "100") int max) {
        return replayService.replay(max);
    }
}
