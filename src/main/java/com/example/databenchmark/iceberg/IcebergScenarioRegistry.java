package com.example.databenchmark.iceberg;

import com.example.databenchmark.iceberg.scenario.AcidTransactionScenario;
import com.example.databenchmark.iceberg.scenario.ConcurrentWriteScenario;
import com.example.databenchmark.iceberg.scenario.ErasureCodingConversionScenario;
import com.example.databenchmark.iceberg.scenario.ErasureCodingScenario;
import com.example.databenchmark.iceberg.scenario.IncrementalPullScenario;
import com.example.databenchmark.iceberg.scenario.RowLevelMutationScenario;
import com.example.databenchmark.iceberg.scenario.SchemaEvolutionScenario;
import com.example.databenchmark.iceberg.scenario.SmallFileCompactionScenario;
import com.example.databenchmark.iceberg.scenario.TimeTravelScenario;
import java.util.List;

public final class IcebergScenarioRegistry {
    private IcebergScenarioRegistry() {}

    public static List<IcebergValidationScenario> defaultScenarios() {
        return List.of(
            new SchemaEvolutionScenario(),
            new ErasureCodingScenario(),
            new ErasureCodingConversionScenario(),
            new ConcurrentWriteScenario(),
            new RowLevelMutationScenario(),
            new AcidTransactionScenario(),
            new IncrementalPullScenario(),
            new TimeTravelScenario(),
            new SmallFileCompactionScenario()
        );
    }
}
