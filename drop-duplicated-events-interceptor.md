# DropDuplicatedEventsInterceptor 

It compares new Events with last events. A set of last Event hashes is maintained, the size of this set can be configured with "size" parameter, default size is 1000.

Event's hash is calculate by default from headers (disabled if "header" parameter is set to false) and body (disabled if "body" parameter is set to false).

WARNINGS: this interceptor will drop not duplicated events in case transaction to channel fails. In case the agent is restarted, hash for last events is lost so duplicates could appear.