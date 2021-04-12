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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.TileDBError;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static io.tiledb.java.api.Datatype.TILEDB_DATETIME_DAY;
import static java.util.Objects.requireNonNull;

/**
 * TileDBModule binds all the class types, declares what is an instance class, and what is a singleton scoped class
 */
public class TileDBModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;

    public TileDBModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);
        configBinder(binder).bindConfig(TileDBConfig.class);
        binder.bind(TileDBClient.class).in(Scopes.SINGLETON);
        binder.bind(TileDBConnectorId.class).toInstance(new TileDBConnectorId(connectorId));
        binder.bind(TileDBMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TileDBSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TileDBRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(TileDBSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(TileDBTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(TileDBColumnProperties.class).in(Scopes.SINGLETON);
        binder.bind(TileDBPageSinkProvider.class).in(Scopes.SINGLETON);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, JsonCodec.listJsonCodec(TileDBTable.class));
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(new TypeSignature(value));
        }
    }

    public static Type prestoTypeFromTileDBType(Datatype type) throws TileDBError
    {
        switch (type) {
            case TILEDB_INT8:
                return TINYINT;
            case TILEDB_UINT8:
                return SMALLINT;
            case TILEDB_INT16:
                return SMALLINT;
            case TILEDB_UINT16:
                return INTEGER;
            case TILEDB_INT32:
                return INTEGER;
            case TILEDB_UINT32:
                return BIGINT;
            case TILEDB_INT64:
                return BIGINT;
            case TILEDB_UINT64:
                return BIGINT;
            case TILEDB_STRING_ASCII:
            case TILEDB_CHAR:
                return VARCHAR;
            case TILEDB_FLOAT32:
                return REAL;
            case TILEDB_FLOAT64:
                return DOUBLE;
            case TILEDB_DATETIME_AS:
            case TILEDB_DATETIME_FS:
            case TILEDB_DATETIME_PS:
            case TILEDB_DATETIME_NS:
            case TILEDB_DATETIME_US:
            case TILEDB_DATETIME_MS:
            case TILEDB_DATETIME_SEC:
            case TILEDB_DATETIME_MIN:
            case TILEDB_DATETIME_HR:
                return TIMESTAMP;
            case TILEDB_DATETIME_DAY:
            case TILEDB_DATETIME_WEEK:
            case TILEDB_DATETIME_MONTH:
            case TILEDB_DATETIME_YEAR:
                return DATE;
            default:
                //TODO: HANDLE ANY and other types
                throw new TileDBError("Unknown type: " + type.toString());
        }
    }

    public static Datatype tileDBTypeFromPrestoType(Type type) throws TileDBError
    {
        type.getJavaType();
        if (type.equals(TINYINT)) {
            return Datatype.TILEDB_INT8;
        }
        else if (type.equals(SMALLINT)) {
            return Datatype.TILEDB_INT16;
        }
        else if (type.equals(INTEGER)) {
            return Datatype.TILEDB_INT32;
        }
        else if (type.equals(BIGINT)) {
            return Datatype.TILEDB_INT64;
        }
        else if (isCharType(type)) {
            return Datatype.TILEDB_CHAR;
        }
        else if (isVarcharType(type)) {
            VarcharType varcharType = ((VarcharType) type);
            // Return TILEDB_CHAR in case the datatype is varchar(1)
            if (!varcharType.isUnbounded() && varcharType.getLengthSafe() == 1) {
                return Datatype.TILEDB_CHAR;
            }
            return Datatype.TILEDB_STRING_ASCII;
        }
        else if (type instanceof VarbinaryType) {
            return Datatype.TILEDB_INT8;
        }
        else if (type.equals(REAL)) {
            return Datatype.TILEDB_FLOAT32;
        }
        else if (type.equals(DOUBLE)) {
            return Datatype.TILEDB_FLOAT64;
        }
        else if (type.equals(DATE)) {
            return TILEDB_DATETIME_DAY;
        }
        else if (type.equals(TIMESTAMP)) {
            return Datatype.TILEDB_DATETIME_MS;
        }
        //TODO: HANDLE ANY and other types
        throw new TileDBError("Unknown type: " + type.toString());
    }

    /**
     * This is a helper function to create an ArrayList for a given type
     * @param type datatype to create list of
     * @param isVariableLength if its variable length we will create a list of arrays
     * @return List
     * @throws TileDBError if the datatype passed is not supported
     */
    public static List<?> getJavaListForType(Datatype type, boolean isVariableLength) throws TileDBError
    {
        switch (type) {
            case TILEDB_FLOAT32: {
                if (isVariableLength) {
                    return new ArrayList<float[]>();
                }
                return new ArrayList<Float>();
            }
            case TILEDB_FLOAT64: {
                if (isVariableLength) {
                    return new ArrayList<double[]>();
                }
                return new ArrayList<Double>();
            }
            case TILEDB_INT8: {
                if (isVariableLength) {
                    return new ArrayList<byte[]>();
                }
                return new ArrayList<Byte>();
            }
            case TILEDB_INT16: {
                if (isVariableLength) {
                    return new ArrayList<short[]>();
                }
                return new ArrayList<Short>();
            }
            case TILEDB_INT32: {
                if (isVariableLength) {
                    return new ArrayList<int[]>();
                }
                return new ArrayList<Integer>();
            }
            case TILEDB_DATETIME_MS:
            case TILEDB_DATETIME_DAY:
            case TILEDB_INT64: {
                if (isVariableLength) {
                    return new ArrayList<long[]>();
                }
                return new ArrayList<Long>();
            }
            case TILEDB_UINT8: {
                if (isVariableLength) {
                    return new ArrayList<short[]>();
                }
                return new ArrayList<Short>();
            }
            case TILEDB_UINT16: {
                if (isVariableLength) {
                    return new ArrayList<int[]>();
                }
                return new ArrayList<Integer>();
            }
            case TILEDB_UINT32: {
                if (isVariableLength) {
                    return new ArrayList<long[]>();
                }
                return new ArrayList<Long>();
            }
            case TILEDB_UINT64: {
                if (isVariableLength) {
                    return new ArrayList<long[]>();
                }
                return new ArrayList<Long>();
            }
            case TILEDB_CHAR: {
                /*if (isVariableLength) {
                    return new ArrayList<String[]>();
                }*/
                return new ArrayList<String>();
            }
            default: {
                throw new TileDBError("Not supported type " + type);
            }
        }
    }
}
