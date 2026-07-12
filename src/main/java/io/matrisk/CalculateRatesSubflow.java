package io.matrisk;

import io.quarkiverse.flow.Flow;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.serverlessworkflow.api.types.FlowDirectiveEnum;
import io.serverlessworkflow.api.types.Workflow;
import io.serverlessworkflow.fluent.func.FuncWorkflowBuilder;
import io.serverlessworkflow.fluent.func.dsl.FuncCallStep;
import io.serverlessworkflow.impl.WorkflowError;
import io.serverlessworkflow.impl.WorkflowException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.serverlessworkflow.fluent.func.dsl.FuncDSL.function;
import static io.serverlessworkflow.fluent.func.dsl.FuncDSL.tasks;
import static io.serverlessworkflow.fluent.func.dsl.FuncDSL.tryCatch;

@ApplicationScoped
public class CalculateRatesSubflow extends Flow {

    private static final Logger log = LoggerFactory.getLogger(CalculateRatesSubflow.class.getName());

    private static final String STOCK_ORDER_ERROR = "ERR_001";
    private static final String PAYMENT_PROCESSING_ERROR = "ERR_002";
    private static final String SHIPPING_ERROR = "ERR_003";

    private static final String ORDER_001 = "ORDER#001";
    private static final String ORDER_002 = "ORDER#002";
    private static final String ORDER_003 = "ORDER#003";
    private static final String END_FLOW = "endFlow";


    @Override
    public Workflow descriptor() {

        
        // 
        FuncCallStep<Phase1Step, CalculationResult> cancelStock = function("cancelStock",
                (Phase1Step o) -> cancelReservation(o.orderId()));
        FuncCallStep<Phase1Step, Phase1Step> cancelPayment = function("cancelPayment",
                (Phase1Step o) -> cancelPayment(o.orderId()));

        return FuncWorkflowBuilder.workflow("calculate-rates-by-curves", "matrisk")
                .tasks(
                        tryCatch(
                                "tryCalculationPhase1",
                                t -> t.tryCatch(function("calculationPhase1", this::reserveStock))
                                        .catchError(
                                                err -> err.type(STOCK_ORDER_ERROR),
                                                function("notifyPhase1Failure", this::notifyPhase1Failure)
                                                        .then(FlowDirectiveEnum.END))),
                        tryCatch(
                                "tryCalculationPhase2",
                                t -> t.tryCatch(function("calculationPhase2", this::processPayment))
                                        .catchWhen(
                                                "${ .status == 503 }",
                                                cancelStock.then(FlowDirectiveEnum.END))),
                        tryCatch(
                                "tryCalculationPhase3",
                                t -> t.tryCatch(function("calculationPhase3", this::scheduleShipping))
                                        .catchType(
                                                SHIPPING_ERROR,
                                                tasks(
                                                        cancelPayment,
                                                        cancelStock))))
                .build();
    }

    private Phase1Step reserveStock(String order) {
        log.info("Reserving stock for order: {}", order);
        broadcastStep(order, "stockReservation", "processing", "Reserving stock...");
        waitMs(500);
        if (order.equals(ORDER_001)) {
            broadcastStep(order, "stockReservation", "failed", "Stock reservation failed - Out of stock");
            throw new WorkflowException(WorkflowError.error(STOCK_ORDER_ERROR, 409).build());
        }
        broadcastStep(order, "stockReservation", "completed", "Stock reserved successfully");
        return new Phase1Step(order, "reserved");
    }

    private Phase1Step processPayment(Phase1Step order) {
        log.info("Processing payment for order: {}", order);
        broadcastStep(order.orderId(), "paymentProcessing", "processing", "Processing payment...");
        waitMs(800);
        if (ORDER_002.equals(order.orderId())) {
            broadcastStep(order.orderId(), "paymentProcessing", "failed", "Payment processing failed - Service unavailable");
            throw new WorkflowException(WorkflowError.error(PAYMENT_PROCESSING_ERROR, 503).build());
        }
        broadcastStep(order.orderId(), "paymentProcessing", "completed", "Payment processed successfully");
        return new Phase1Step(order.orderId(), "paid");
    }

    private Phase1Step scheduleShipping(Phase1Step order) {
        log.info("Scheduling shipping for order: {}", order);
        broadcastStep(order.orderId(), "shipping", "processing", "Scheduling shipping...");
        waitMs(800);
        if (ORDER_003.equals(order.orderId())) {
            broadcastStep(order.orderId(), "shipping", "failed", "Shipping failed - Carrier unavailable");
            throw new WorkflowException(WorkflowError.error(SHIPPING_ERROR, 500).build());
        }
        broadcastStep(order.orderId(), "shipping", "completed", "Shipping scheduled successfully");
        return new Phase1Step(order.orderId(), "shipping");
    }

    private CalculationResult notifyPhase1Failure(String order) {
        log.info("Stock reservation failed for order: {}, there is nothing to compensate", order);
        broadcastStep(order, "compensation", "failed", "Stock unavailable — no reservation to cancel");
        return new CalculationResult("error");
    }

    private CalculationResult cancelReservation(String order) {
        log.info("Cancelling stock reservation for order: {}", order);
        broadcastStep(order, "compensation", "processing", "Cancelling stock reservation...");
        waitMs(400);
        broadcastStep(order, "compensation", "completed", "Stock reservation cancelled");
        return new CalculationResult("error");
    }

    private Phase1Step cancelPayment(String order) {
        log.info("Cancel payment for order: {}", order);
        broadcastStep(order, "compensation", "processing", "Cancelling payment...");
        waitMs(400);
        broadcastStep(order, "compensation", "completed", "Payment cancelled");
        return new Phase1Step(order, "error");
    }

    private void broadcastStep(String orderId, String step, String status, String message) {
        String json = String.format(
                "{\"orderId\":\"%s\",\"step\":\"%s\",\"status\":\"%s\",\"message\":\"%s\"}",
                orderId, step, status, message);
    }

    private static void waitMs(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @RegisterForReflection
    public record Phase1Step(String orderId, String status) {
    }

    @RegisterForReflection
    public record CalculationResult(String status) {
    }
}
