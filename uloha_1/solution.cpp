#ifndef __PROGTEST__

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <climits>
#include <cfloat>
#include <cmath>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <vector>
#include <set>
#include <list>
#include <map>
#include <unordered_set>
#include <unordered_map>
#include <queue>
#include <stack>
#include <deque>
#include <memory>
#include <functional>
#include <thread>
#include <mutex>
#include <semaphore>
#include <atomic>
#include <condition_variable>
#include "progtest_solver.h"
#include "sample_tester.h"

#endif /* __PROGTEST__ */

#define PTDEBUG 0

#define DEBUG_THREADS 1
#define DEBUG_SIZE 2

//default 3
#define START_THREADS 3
//default 2
#define CUSTOMER_ORDERS 5


enum class SolverState {
    NotFull,
    FullButWaiting,
    Blocked,
    Solved
};

std::atomic<bool> correctOutput = true;

std::ostream& operator<<(std::ostream& os, SolverState state) {
    switch (state) {
        case SolverState::NotFull:
            os << "NotFull";
            break;
        case SolverState::FullButWaiting:
            os << "FullButWaiting";
            break;
        case SolverState::Solved:
            os << "Solved";
            break;
        case SolverState::Blocked:
            os << "Blocked";
            break;
    }
    return os;
}

class CSolverInfo
{
public:
    AProgtestSolver solver;
    SolverState state = SolverState::NotFull;
    uint threadsConnected;

    CSolverInfo(AProgtestSolver solver, SolverState state, int threadsConnected)
            : solver(solver), state(state), threadsConnected(threadsConnected) {}
};

class COrderWrapper
{
public:
    uint ordersResolved = 0;
    ACustomer customer;
    AOrderList origin;

    COrderWrapper(): ordersResolved(0), customer(nullptr), origin(nullptr) {}
    COrderWrapper(AOrderList origin, int ordersResolved, ACustomer customer)
            : ordersResolved(ordersResolved), customer(customer), origin(origin) {}
};


class CWeldingCompany {
public:
    // locks
    std::mutex universal;
    std::mutex producerMtx;

    std::vector<std::thread> workingThreads;
    std::vector<std::thread> customerThreads;

    // all available PTSolvers
    std::vector<std::shared_ptr<CSolverInfo>> activeSolvers;

    std::set<ACustomer> customers;
    std::set<AProducer> producers;

    // materialID -> priceList
    std::map<uint, APriceList> materialPrice;

    // used to get all orders from a Solver
    std::map<AProgtestSolver, std::shared_ptr<std::vector<std::pair<std::shared_ptr<COrder>, ACustomer>>>> solverPropertyMap;

    std::atomic<bool> stopCalled = false;


    // used to get orderlist of an order
    std::map<std::shared_ptr<COrder>, std::shared_ptr<COrderWrapper>> orderParent;

    std::condition_variable workerIdeling;

    std::condition_variable cv;
    std::atomic<int> activeProducers = 0;

    static bool usingProgtestSolver() {
        return true;
    }

    static void seqSolve(APriceList priceList,
                         COrder &order) {
        // empty, using progtest solver
    }

    void addProducer(AProducer prod)
    {
//        printf("lock in addProducer\n");
        std::lock_guard<std::mutex> lock(universal);
        producers.insert(prod);
//        printf("exiting lock addProducer\n");
    }

    void addCustomer(ACustomer cust)
    {
        customers.insert(cust);
    }

    void addPriceList(AProducer prod,
                      APriceList priceList)
    {
//        printf("adding price list\n");
        // only add if it is better price
//        std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 51));
//        printf("lock in addPriceList\n");
        std::unique_lock<std::mutex> lock(producerMtx);
        auto it = materialPrice.find(priceList->m_MaterialID);

        // material exists, update values
        if (it != materialPrice.end()) {
            for(const auto newProd : priceList->m_List)
            {
                bool found = false;
                for (auto &existingProd : it->second->m_List) {
                    if ((existingProd.m_W == newProd.m_W && existingProd.m_H == newProd.m_H)
                        || (existingProd.m_W == newProd.m_H && existingProd.m_H == newProd.m_W))
                    {
//                        printf("updating price %f %f\n", existingProd.m_Cost, newProd.m_Cost);
                        if (existingProd.m_Cost > newProd.m_Cost) {

                            existingProd.m_Cost = newProd.m_Cost;
                        }
                        found = true;
                        break;
                    }

                }
                if(!found) {
                    it->second->m_List.push_back(newProd);
                }
            }
        }
        // add new material
        else
        {
            materialPrice.insert({priceList->m_MaterialID, priceList});
        }

        activeProducers--;
        if(activeProducers == 0) {
            cv.notify_all();
        }
        lock.unlock();
//        printf("exiting lock addPriceList\n");
    }




    void start(unsigned thrCount)
    {
        if(thrCount < DEBUG_THREADS)
            throw std::invalid_argument("thrCount < DEBUG_THREADS");

        for(auto &customer : customers)
        {
            customerThreads.push_back(std::thread(&CWeldingCompany::customerInit, this, customer));
        }
//        printf("customer threads started %d\n", customerThreads.size());

//
        for (unsigned i = 0; i < thrCount; i++) {
            workingThreads.push_back(std::thread(&CWeldingCompany::work, this));
        }

//        printf("worker threads started %d\n", workingThreads.size());
    }



    void work()
    {
        while(!stopCalled)
        {
//            std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 51)); // 0 to 50 ms


            std::unique_lock<std::mutex> lock0(universal);

            std::shared_ptr<CSolverInfo> currentSolver = nullptr;
            for(auto solver : activeSolvers) {
                if(solver->state == SolverState::FullButWaiting && solver->threadsConnected < solver->solver->totalThreads()){
//                    printf("current active solvers in %d\n", activeSolvers.size());
                    currentSolver = solver;
                    break;
                }
            }
            if(currentSolver == nullptr) {
                // TODO - COULD BE CONDITIONAL VARIABLE - notify all and stuff
                workerIdeling.wait(lock0);

                continue;
            }
//            printf("lock in work %d\n", this_id);
            currentSolver->threadsConnected++;
//            printf("threads connected after increment %d %d\n", currentSolver->threadsConnected, this_id);

            if(currentSolver->threadsConnected == currentSolver->solver->totalThreads())
            {
//                printf("solver full %d\n", this_id);
                currentSolver->state = SolverState::Blocked;
            }
//            printf("current active solvers out %d\n", activeSolvers.size());
//            printf("exiting lock work first %d \n", this_id);
            lock0.unlock();


            // todo - check error output
            currentSolver->solver->solve();

            lock0.lock();
//            printf("lock in work %d\n", this_id);
            currentSolver->threadsConnected--;
//            printf("before if %d, %d, %d \n", currentSolver->threadsConnected, currentSolver->state, this_id);

            if(currentSolver->threadsConnected == 0 && currentSolver->state == SolverState::Blocked) {
//                printf("solver solved -- propagating result %d\n", this_id);
                currentSolver->state = SolverState::Solved;
                resolveSolver(currentSolver);

            }
//            printf("exiting lock work second %d\n", this_id);
            lock0.unlock();
//            printf("%d, all resolved -- moving to next solver\n", this_id);
        }
//        printf( "stop called, exiting thread %d\n", this_id);
    }

    // for each order - increment order wrapper and check if all orders are resolved
    // if so, send the orderlist to the customer
    void resolveSolver(std::shared_ptr<CSolverInfo> currentSolver)
    {
        auto vectorPtr = solverPropertyMap[currentSolver->solver];
        for(auto order : *vectorPtr) {
            std::shared_ptr<COrder> orderPtr = order.first;

            if (orderParent.find(orderPtr) != orderParent.end()) {
                orderParent[orderPtr]->ordersResolved++;
//                printf("order resolved %d\n", orderParent[orderPtr]->ordersResolved);
                if(orderParent[orderPtr]->ordersResolved == orderParent[orderPtr]->origin->m_List.size()) {
#if PTDEBUG
                    bool tmp = orderParent[orderPtr]->customer->completed(orderParent[orderPtr]->origin);
                    if(!tmp) {
                        for(auto &order : orderParent[orderPtr]->origin->m_List) {
//                            printf("order: %f\n", order.m_Cost);
                        }
                        correctOutput = false;
                    }
#else
                    orderParent[orderPtr]->customer->completed(orderParent[orderPtr]->origin);
#endif
                }
            }
        }
    }


    void fetchMaterialPrices(uint id) {

//        printf("lock in fetch\n");
        std::lock_guard<std::mutex> lock(universal);
        std::unique_lock<std::mutex> lockProducer(producerMtx);
        if(materialPrice.find(id) == materialPrice.end()) {
            lockProducer.unlock();

            activeProducers = producers.size();
//            printf("start producer threads\n");

            for(auto producer : producers) {
//                printf("calling for sendPriceList id: %d\n", id);

                producer->sendPriceList(id);
            }

            lockProducer.lock();
            cv.wait(lockProducer, [this] { return activeProducers == 0; });


//            printf("stop producer threads\n");
        }
//        printf("exiting lock fetch\n");
    }

    // creates and fills PTSolvers
    void storeDemand(AOrderList orderList, ACustomer customer) {

        std::shared_ptr<COrderWrapper> wrapper = std::make_shared<COrderWrapper>(orderList, 0, customer);


//        printf("storing demand with %d orders\n", orderList->m_List.size());
//        printf("lock in storedemand\n");
        std::unique_lock<std::mutex> lock2(universal);
        for(auto &order : orderList->m_List) {
            if(activeSolvers.empty() || !activeSolvers[activeSolvers.size()-1]->solver->hasFreeCapacity()) {
#if PTDEBUG
                activeSolvers.push_back(std::make_shared<CSolverInfo>(createProgtestSolverDebug(DEBUG_SIZE, DEBUG_THREADS), SolverState::NotFull, 0));
#else
                activeSolvers.push_back(std::make_shared<CSolverInfo>(createProgtestSolver(), SolverState::NotFull, 0));
#endif
            }
            auto currentSolver = activeSolvers[activeSolvers.size()-1];

            currentSolver->solver->addProblem(materialPrice[orderList->m_MaterialID], order);

            if(!currentSolver->solver->hasFreeCapacity()) {
                currentSolver->state = SolverState::FullButWaiting;
                workerIdeling.notify_all();
            }

            auto tmp = std::make_shared<COrder>(order);
            if(solverPropertyMap.find(currentSolver->solver) == solverPropertyMap.end())
                solverPropertyMap[currentSolver->solver] = std::make_shared<std::vector<std::pair<std::shared_ptr<COrder>, ACustomer>>>();
            solverPropertyMap[currentSolver->solver]->push_back({tmp, customer});
            orderParent.insert({tmp, wrapper});

        }
        lock2.unlock();
//        printf("exiting lock storeDemand\n");
    }

    void customerInit(ACustomer customer)
    {
        while(true)
        {
//            std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 51)); // 0 to 50 ms
//            printf("customer: reading demand\n");
            AOrderList orderList = customer->waitForDemand();

            if (orderList == nullptr) {
//                printf("customer: no more customer order\n");
                return;
            }
            else
            {
//                printf("customer: demand read\n");
                fetchMaterialPrices(orderList->m_MaterialID);
                storeDemand(orderList, customer);
//                printf("customer: demand stored\n");
            }
        }
    }

    bool allSolved()
    {
//        printf("ALLSOLVED KILLED\n");
//        return true;


        std::lock_guard<std::mutex> lock(universal);
        for(auto solver : activeSolvers) {
            if(solver->state != SolverState::Solved) {

                return false;
            }
        }
        return true;

    }

    void stop()
    {
        for(auto &thread : customerThreads)
        {
            thread.join();
        }

//        printf("all customer threads stopped\n");

//        printf("lock in stop\n");
        std::unique_lock<std::mutex> lock(universal);
        for(auto solver : activeSolvers) {

            if(solver->state == SolverState::NotFull) {
//                printf("state changed from NotFull\n");
                solver->state = SolverState::FullButWaiting;
                workerIdeling.notify_all();
            }

        }
        lock.unlock();
//        printf("exiting lock stop\n");

        //todo conditional wait asi
        while(!allSolved()) {
            std::this_thread::sleep_for(std::chrono::milliseconds (2));
        }

        stopCalled = true;

        workerIdeling.notify_all();

//        printf("resolving not full solvers\n");
        uint tmp = 0;

        this->printState();

        for(auto &thread : workingThreads) {
            thread.join();
            tmp++;
//            printf("%d worker threads joined\n", tmp);
        }
//        printf("worker threads stopped\n");
    }

    void printState()
    {
        return;
        std::lock_guard<std::mutex> lock(universal);

        std::cout << "**********STATE**********" << std::endl;
        std::cout << "active solvers: " << activeSolvers.size() << std::endl;
        for(auto solver : activeSolvers) {
            std::cout << "solver state: " << solver->state << std::endl;
        }

        std::cout << "customers: " << customers.size() << std::endl;
        std::cout << "producers: " << producers.size() << std::endl;
        std::cout << "order parent: " << orderParent.size() << std::endl;
        std::cout << "workerThread size: " << workingThreads.size() << std::endl;
        std::cout << "customerThread size: " << customerThreads.size() << std::endl;
        std::cout << "stop called: " << stopCalled << std::endl;
        std::cout << "**********STATE**********" << std::endl;
    }
};


//-------------------------------------------------------------------------------------------------
#ifndef __PROGTEST__

void testPT()
{

    using namespace std::placeholders;

    CWeldingCompany test;

    AProducer p1 = std::make_shared<CProducerSync>(std::bind(&CWeldingCompany::addPriceList, &test, _1, _2));
    AProducerAsync p2 = std::make_shared<CProducerAsync>(std::bind(&CWeldingCompany::addPriceList, &test, _1, _2));
    AProducerAsync p3 = std::make_shared<CProducerAsync>(std::bind(&CWeldingCompany::addPriceList, &test, _1, _2));

    test.addProducer(p1);
    test.addProducer(p2);
    test.addProducer(p3);
    test.addCustomer(std::make_shared<CCustomerTest>(CUSTOMER_ORDERS));
    p2->start();
    p3->start();

    test.start(START_THREADS);
//    test.printState();
//    std::this_thread::sleep_for(std::chrono::milliseconds (200));

    test.stop();
    p2->stop();
    p3->stop();

#if PTDEBUG

// print test.materialPrice
    for(auto &material : test.materialPrice) {
        std::cout << "Material: " << material.first << std::endl;
        for(auto &prod : material.second->m_List) {
            std::cout << "W: " << prod.m_W << " H: " << prod.m_H << " Cost: " << prod.m_Cost << std::endl;
        }
    }
    if(correctOutput)
        std::cout << "CORRECT" << std::endl;
    else {

        std::cout << "INCORRECT" << std::endl;
    }
#endif
}



int main() {
//    unitTest();

    //TODO - COMPLETE SHOULDNT RETURN ANYTHING, remove all sleeps, remove all prints, remove useless vars

    testPT();


    return EXIT_SUCCESS;
}



#endif /* __PROGTEST__ */
