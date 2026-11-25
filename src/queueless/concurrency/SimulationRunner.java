package queueless.concurrency;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import queueless.exception.QueueLessException;
import queueless.model.Customer;
import queueless.model.Priority;
import queueless.model.ServiceCounter;
import queueless.model.ServiceType;
import queueless.model.Token;
import queueless.service.CounterService;
import queueless.service.CustomerService;
import queueless.service.TokenService;
import queueless.util.Console;

/**
 * Producer-consumer demo: several kiosk (producer) threads feed the {@link TokenQueue} while one
 * counter (consumer) thread per open counter drains it. Coordination is done with wait()/notifyAll()
 * inside TokenQueue; the thread bodies are Runnable lambdas.
 */
public class SimulationRunner {

    private static final String[] DEMO_NAMES = {
        "Aarav Mehta", "Diya Kapoor", "Kabir Singh", "Meera Nair",
        "Rohan Gill", "Simran Kaur", "Vivaan Joshi", "Anaya Bose"
    };

    private final TokenService tokenService;
    private final CounterService counterService;
    private final CustomerService customerService;
    private final TokenQueue queue;

    public SimulationRunner(TokenService tokenService, CounterService counterService,
                            CustomerService customerService, TokenQueue queue) {
        this.tokenService = tokenService;
        this.counterService = counterService;
        this.customerService = customerService;
        this.queue = queue;
    }

    public void run(int producerCount, int tokensPerProducer) throws QueueLessException {
        ensureDemoCustomers();
        List<Integer> ids = customerService.listAll().stream().map(Customer::getId).collect(Collectors.toList());

        queue.reopen();
        AtomicInteger issued = new AtomicInteger();
        AtomicInteger served = new AtomicInteger();

        List<Thread> workerThreads = new ArrayList<>();
        for (ServiceCounter c : counterService.getCounters()) {
            if (!c.isOpen() || c.isBusy()) continue;
            workerThreads.add(new Thread(() -> counterLoop(c, served), "worker-" + c.getId()));
        }
        List<Thread> producerThreads = new ArrayList<>();
        for (int i = 1; i <= producerCount; i++) {
            String kiosk = "Kiosk " + i;
            producerThreads.add(new Thread(() -> kioskLoop(kiosk, ids, tokensPerProducer, issued), "producer-" + i));
        }

        Console.info("Starting " + producerThreads.size() + " kiosks and " + workerThreads.size() + " counters...");
        workerThreads.forEach(Thread::start);
        producerThreads.forEach(Thread::start);
        try {
            for (Thread t : producerThreads) t.join();
            queue.close();                                  // no more arrivals: wake all waiting counters
            for (Thread t : workerThreads) t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            queue.close();
        } finally {
            queue.reopen();
        }

        Console.line();
        Console.success("Simulation finished: " + issued.get() + " tokens issued, " + served.get() + " served, "
                + queue.size() + " still waiting (no open counter for their service).");
    }

    /** Producer thread body: customers "arrive" and receive tokens at random intervals. */
    private void kioskLoop(String name, List<Integer> customerIds, int target, AtomicInteger issuedTotal) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        ServiceType[] types = ServiceType.values();
        int issued = 0;
        int attempts = 0;
        try {
            while (issued < target && attempts < target * 6) {
                attempts++;
                int customerId = customerIds.get(random.nextInt(customerIds.size()));
                ServiceType type = types[random.nextInt(types.length)];
                try {
                    Token t = tokenService.issueTokenBlocking(customerId, type);
                    issued++;
                    issuedTotal.incrementAndGet();
                    Console.println("  [" + name + "] issued " + t.getTokenNumber() + " (" + t.getPriority() + ")");
                } catch (QueueLessException e) {
                    // e.g. this customer already waits for this service: just try another arrival
                }
                Thread.sleep(80 + random.nextInt(160));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Console.info(name + " interrupted");
        }
    }

    /** Consumer thread body: one worker per counter keeps calling and serving tokens until the queue is closed. */
    private void counterLoop(ServiceCounter counter, AtomicInteger servedTotal) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int served = 0;
        try {
            Token token;
            while ((token = counterService.callNextBlocking(counter.getId())) != null) {
                Console.println("  [" + counter.getName() + "] now serving " + token.getTokenNumber()
                        + " (" + token.getPriority() + ")");
                Thread.sleep(500 + random.nextInt(700));            // simulated service time
                counterService.completeService(counter.getId());
                served++;
                servedTotal.incrementAndGet();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (QueueLessException e) {
            Console.error(counter.getName() + " stopped: " + e.getMessage());
        }
        Console.println("  [" + counter.getName() + "] closed for the day, served " + served);
    }

    private void ensureDemoCustomers() throws QueueLessException {
        Priority[] cycle = Priority.values();
        for (int i = 0; i < DEMO_NAMES.length; i++) {
            try {
                customerService.register(DEMO_NAMES[i], "980000000" + i, 25 + i * 5, cycle[i % cycle.length]);
            } catch (QueueLessException e) {
                if (!e.is(QueueLessException.DUPLICATE_CUSTOMER)) throw e;   // demo customer already exists
            }
        }
    }
}
