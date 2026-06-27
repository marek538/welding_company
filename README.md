# Multi-threaded Order Processing (Welding Company)

## Project Description
A multi-threaded C++ simulation of a manufacturing company. The system asynchronously processes customer orders for welded steel plates, dynamically fetches material price lists from multiple suppliers, and calculates the optimal (cheapest) manufacturing cost based on available prefabricated parts and welding prices.

## Solution & Algorithm
The architecture heavily relies on the **Producer-Consumer pattern** and a custom **Thread Pool**.
* **Concurrency:** Managed via `std::mutex`, `std::unique_lock`, and `std::condition_variable` to safely synchronize shared resources (price lists, pending orders, active solvers) between supplier threads, customer threads, and worker threads.
* **Batch Processing:** Utilizes a provided solver (`AProgtestSolver`) that requires batching multiple problems and synchronizing a strict number of worker threads to execute the evaluation simultaneously.
* **Underlying Algorithm (`AProgtestSolver`):** The core calculation (finding the cheapest combination of smaller plates to build a larger one) is a variation of the 2D Cutting Stock problem, solved using Dynamic Programming.

