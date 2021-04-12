/*
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
package com.facebook.presto.plugin.tiledb;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * Factory for creating TileDBConnectors
 */
public class TileDBConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "tiledb";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new TileDBHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new TileDBModule(catalogName, context.getTypeManager()),
                    binder -> {
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            // Create instances for connector
            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            TileDBMetadata metadata = injector.getInstance(TileDBMetadata.class);
            TileDBSplitManager splitManager = injector.getInstance(TileDBSplitManager.class);
            TileDBRecordSetProvider recordSetProvider = injector.getInstance(TileDBRecordSetProvider.class);
            TileDBSessionProperties tileDBSessionProperties = injector.getInstance(TileDBSessionProperties.class);

            TileDBTableProperties tileDBTableProperties = injector.getInstance(TileDBTableProperties.class);
            TileDBColumnProperties tileDBColumnProperties = injector.getInstance(TileDBColumnProperties.class);
            TileDBPageSinkProvider pageSinkProvider = injector.getInstance(TileDBPageSinkProvider.class);
            return new TileDBConnector(
                    lifeCycleManager,
                    metadata,
                    splitManager,
                    recordSetProvider,
                    tileDBSessionProperties.getSessionProperties(),
                    tileDBTableProperties.getTableProperties(),
                    tileDBColumnProperties.getColumnProperties(),
                    pageSinkProvider);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new PrestoException(TileDBErrorCode.TILEDB_CONNECTOR_ERROR, e);
        }
    }
}
