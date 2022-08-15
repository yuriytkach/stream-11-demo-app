package com.yuriytkach.demo.stream11;

import java.util.UUID;

public record Order(UUID id, String itemName, Integer userId) {
}
