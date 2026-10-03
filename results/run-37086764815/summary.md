| scenario | variant | runs | seconds (median) | shuffle read MB | spill MB | slowest / median task | rows | matches naive |
|---|---|---:|---:|---:|---:|---:|---:|---|
| duplicate_key | naive | 3 | 56.3 | 885 | 17745 | 12.9 | 20000000 | yes |
| duplicate_key | right | 3 | 5.1 | 15 | 0 | 1.9 | 20000000 | yes |
| duplicate_key | wrong | 3 | 56.1 | 893 | 17140 | 11.0 | 20000000 | yes |
| hot_key | naive | 3 | 8.5 | 390 | 0 | 36.9 | 20000000 | yes |
| hot_key | right | 3 | 7.9 | 390 | 0 | 1.6 | 20000000 | yes |
| hot_key | wrong | 3 | 11.8 | 390 | 0 | 185.2 | 20000000 | yes |
| legitimate_fanout | naive | 3 | 19.1 | 400 | 0 | 11.8 | 400000 | yes |
| legitimate_fanout | right | 3 | 4.3 | 33 | 0 | 1.8 | 400000 | yes |
| legitimate_fanout | wrong | 3 | 20.7 | 405 | 0 | 8.6 | 400000 | yes |
| weak_window_key | naive | 3 | 16.6 | 418 | 0 | 2.3 | 20000000 | n/a (key changed) |
| weak_window_key | right | 3 | 15.2 | 569 | 0 | 1.5 | 20000000 | n/a (key changed) |
| weak_window_key | wrong | 3 | 17.6 | 406 | 0 | 1.5 | 20000000 | n/a (key changed) |
