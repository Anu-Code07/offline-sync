# DevTools

`sync_devtools` provides a Flutter panel for inspecting:

- current sync state
- pending mutation queue
- retry attempts and last errors
- dead-letter queue
- sync timeline events
- websocket-triggered invalidations

The first implementation is a widget-level inspector so apps can embed it behind
a debug flag. A future extension can expose the same data through the Flutter
DevTools extension protocol.
