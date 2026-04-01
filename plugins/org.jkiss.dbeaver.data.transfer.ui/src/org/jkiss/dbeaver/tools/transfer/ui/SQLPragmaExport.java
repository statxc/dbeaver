/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.tools.transfer.ui;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.data.json.JSONUtils;
import org.jkiss.dbeaver.model.impl.PropertyDescriptor;
import org.jkiss.dbeaver.model.preferences.DBPPropertyDescriptor;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLPragmaHandler;
import org.jkiss.dbeaver.model.sql.commands.SQLCommandExportBegin;
import org.jkiss.dbeaver.model.struct.DBSDataContainer;
import org.jkiss.dbeaver.tasks.ui.wizard.TaskConfigurationWizardDialog;
import org.jkiss.dbeaver.tools.transfer.*;
import org.jkiss.dbeaver.tools.transfer.database.DatabaseTransferProducer;
import org.jkiss.dbeaver.tools.transfer.registry.DataTransferNodeDescriptor;
import org.jkiss.dbeaver.tools.transfer.registry.DataTransferProcessorDescriptor;
import org.jkiss.dbeaver.tools.transfer.registry.DataTransferRegistry;
import org.jkiss.dbeaver.tools.transfer.stream.StreamTransferConsumer;
import org.jkiss.dbeaver.tools.transfer.ui.wizard.DataTransferWizard;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.utils.CommonUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class SQLPragmaExport implements SQLPragmaHandler {
    private static final Log log = Log.getLog(SQLPragmaExport.class);

    public static final String PARAMETER_TYPE = "type";
    public static final String PARAMETER_INCLUDE_PIPES = "includePipesConfiguration";

    private static final String PRODUCER_NODE_ID = "database_producer";
    private static final String CONSUMER_NODE_ID = "stream_consumer";
    private static final String PROCESSOR_ID_PREFIX = CONSUMER_NODE_ID + ":stream.";

    private static final String KEY_PIPE_INDEX = "_pipeIndex";
    private static final String KEY_SETTINGS = "_settings";
    private static final String KEY_RUNTIME_PARAMS = "_runtimeParams";

    @Override
    public int processPragma(
        @NotNull DBRProgressMonitor monitor,
        @NotNull DBSDataContainer container,
        @NotNull Map<String, Object> parameters
    ) throws DBException {
        boolean beginMode = CommonUtils.toBoolean(parameters.get(SQLCommandExportBegin.BEGIN_MODE_KEY));

        if (beginMode) {
            return processDirectExport(monitor, container, parameters);
        } else {
            return processWizardExport(container, parameters);
        }
    }

    private int processWizardExport(
        @NotNull DBSDataContainer container,
        @NotNull Map<String, Object> parameters
    ) throws DBException {
        DataTransferSettings settings = createTransferSettings(container, parameters);
        final boolean includePipes = JSONUtils.getBoolean(parameters, PARAMETER_INCLUDE_PIPES, false);

        UIUtils.asyncExec(() -> {
            final DataTransferWizard wizard = new DataTransferWizard(null, settings, true) {
                @Override
                protected boolean includePipesConfigurationPage() {
                    return includePipes;
                }
            };

            new TaskConfigurationWizardDialog(UIUtils.getActiveWorkbenchWindow(), wizard).open();
        });

        return RESULT_CONSUME_PRAGMA | RESULT_CONSUME_QUERY;
    }

    private int processDirectExport(
        @NotNull DBRProgressMonitor monitor,
        @NotNull DBSDataContainer container,
        @NotNull Map<String, Object> parameters
    ) throws DBException {
        // Track the pipe index across invocations so that useSingleFile/appendToFile
        // work correctly for multiple queries within an @export_begin block
        int pipeIndex = 0;
        Object storedIndex = parameters.get(KEY_PIPE_INDEX);
        if (storedIndex instanceof Integer idx) {
            pipeIndex = idx;
        }

        DataTransferSettings settings = createTransferSettings(container, parameters);
        settings.loadNodeSettings(monitor);

        List<DataTransferPipe> pipes = settings.getDataPipes();
        if (pipes.isEmpty()) {
            throw new DBException("No data transfer pipes configured");
        }

        DataTransferPipe pipe = pipes.get(0);
        // Use the accumulated pipeIndex so the consumer sees the correct orderNumber.
        // This makes useSingleFile append subsequent queries to the first file.
        pipe.initPipe(settings, pipeIndex, pipeIndex + 1);

        IDataTransferConsumer<?, ?> consumer = pipe.getConsumer();
        IDataTransferSettings consumerNodeSettings = settings.getNodeSettings(settings.getConsumer());

        // Reuse runtime parameters from prior query so that the output file name
        // is consistent across all queries in the block
        Object storedRuntimeParams = parameters.get(KEY_RUNTIME_PARAMS);
        if (storedRuntimeParams != null) {
            consumer.setRuntimeParameters(storedRuntimeParams);
        } else if (consumerNodeSettings != null) {
            Object runtimeParams = consumerNodeSettings.prepareRuntimeParameters();
            consumer.setRuntimeParameters(runtimeParams);
            parameters.put(KEY_RUNTIME_PARAMS, runtimeParams);
        }

        consumer.startTransfer(monitor);

        try {
            IDataTransferProducer<?> producer = pipe.getProducer();
            IDataTransferSettings producerNodeSettings = settings.getNodeSettings(settings.getProducer());
            IDataTransferProcessor processor = settings.getProcessor() == null
                ? null : settings.getProcessor().getInstance();

            producer.transferData(monitor, consumer, processor, producerNodeSettings, null);
            consumer.finishTransfer(monitor, false);
        } catch (Exception e) {
            consumer.finishTransfer(monitor, e, null, false);
            if (e instanceof DBException dbException) {
                throw dbException;
            }
            throw new DBException("Data transfer failed", e);
        }

        // Increment the pipe index for the next query
        parameters.put(KEY_PIPE_INDEX, pipeIndex + 1);

        // Do NOT consume the pragma so it persists for the next query
        return RESULT_CONSUME_QUERY;
    }

    @NotNull
    private DataTransferSettings createTransferSettings(
        @NotNull DBSDataContainer container,
        @NotNull Map<String, Object> parameters
    ) throws DBException {
        final String type = JSONUtils.getString(parameters, PARAMETER_TYPE);
        if (CommonUtils.isEmpty(type)) {
            throw new DBException("`type` attribute is mandatory");
        }

        final DataTransferRegistry registry = DataTransferRegistry.getInstance();
        final DataTransferNodeDescriptor producerNode = registry.getNodeById(PRODUCER_NODE_ID);
        final DataTransferNodeDescriptor consumerNode = registry.getNodeById(CONSUMER_NODE_ID);
        final DataTransferProcessorDescriptor processor = registry.getProcessor(PROCESSOR_ID_PREFIX + type);

        if (processor == null) {
            throw new DBException("Can't find processor of type '" + type + "'");
        }

        return new DataTransferSettings(
            Collections.singleton(new DatabaseTransferProducer(container, null)),
            Collections.singleton(new StreamTransferConsumer()),
            Map.of(
                DTConstants.PROP_PRODUCER_TYPE, producerNode.getId(),
                DTConstants.PROP_CONSUMER_TYPE, consumerNode.getId(),
                DTConstants.PROP_PROCESSOR_TYPE, processor.getId(),
                DTConstants.PROP_PROCESSORS_LIST, Map.of(
                    processor.getFullId(), createProcessorSettings(processor, parameters)
                ),
                producerNode.getNodeClass().getSimpleName(), createProducerSettings(parameters),
                consumerNode.getNodeClass().getSimpleName(), createConsumerSettings(parameters)
            ),
            new DataTransferState(),
            true,
            true,
            false,
            false
        );
    }

    @NotNull
    private static Map<String, Object> createProducerSettings(@NotNull Map<String, Object> parameters) {
        return JSONUtils.getObject(parameters, DTConstants.PROP_PRODUCER_TYPE);
    }

    @NotNull
    private static Map<String, Object> createConsumerSettings(
        @NotNull Map<String, Object> parameters) {
        return JSONUtils.getObject(parameters, DTConstants.PROP_CONSUMER_TYPE);
    }

    @NotNull
    private static Map<String, Object> createProcessorSettings(
        @NotNull DataTransferProcessorDescriptor processor,
        @NotNull Map<String, Object> parameters
    ) {
        final Map<String, Object> properties = new HashMap<>();
        final StringJoiner names = new StringJoiner(",");

        for (DBPPropertyDescriptor property : processor.getProperties()) {
            properties.put(property.getId(), property.getDefaultValue());
            names.add(property.getId());
        }

        for (Map.Entry<String, Object> property : JSONUtils.getObject(parameters, DTConstants.PROP_PROCESSOR_TYPE).entrySet()) {
            final DBPPropertyDescriptor propertyDescriptor = processor.getProperty(property.getKey());

            if (propertyDescriptor == null) {
                log.debug("Skipping unknown property " + property.getKey());
                continue;
            }

            properties.put(property.getKey(), PropertyDescriptor.convertString(CommonUtils.toString(property.getValue()), propertyDescriptor.getDataType()));
        }

        properties.put(DTConstants.PROP_NAME, names.toString());

        return properties;
    }
}
