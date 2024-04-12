# Guildelines

## Purpose of These Guidelines

The orquestra-sdk repository is utilized within both client and server contexts. 
To streamline the development and release processes, we organize code functionalities 
into subdirectories.

## Where Should I Place My Code?

There are three main directories within the codebase:

- _shared
- _runtime
- _client

The remaining directories serve as pure shims with the sole purpose of exposing the API to the public.

The primary guideline for determining where to write your code involves asking yourself two questions:

1. Will my code be used on the server side or the client side?
2. Does my code involve an interface between the client and server?

If your code is exclusively for the client side, place it in the _client directory.
If your code is intended for the server side and does not contain interfaces (such as shared JSON models), place it in the _runtime directory.
If your code involves an interface that must match between the client and server, utilize the _shared directory.
