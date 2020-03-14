/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright (c) $year
 * Date: 3/2/19 12:03 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common;

import com.codekutter.common.utils.CollectionUtils;
import com.codekutter.common.utils.ReflectionUtils;
import com.codekutter.zconfig.common.model.Configuration;
import com.codekutter.zconfig.common.model.EncryptedValue;
import com.codekutter.zconfig.common.model.annotations.*;
import com.codekutter.zconfig.common.model.nodes.*;
import com.codekutter.zconfig.common.transformers.NullParser;
import com.codekutter.zconfig.common.transformers.NullTransformer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.reflect.MethodUtils;

import javax.annotation.Nonnull;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Helper class to auto-apply configuration values to annotated types.
 */
public class ConfigurationAnnotationProcessor {
    /**
     * Reader and apply the values from the passed configuration based on the type annotations.
     *
     * @param type   - Type of the target object.
     * @param config - Configuration source.
     * @param target - Target to apply the values to.
     * @param <T>    - Annotated object type.
     * @return - Updated target instance.
     * @throws ConfigurationException
     */
    public static <T> T readConfigAnnotations(@Nonnull Class<? extends T> type,
                                              @Nonnull Configuration config,
                                              @Nonnull T target)
            throws ConfigurationException {
        return readConfigAnnotations(type, config, target, null, null);
    }

    /**
     * Reader and apply the values from the passed configuration based on the type annotations.
     *
     * @param type   - Type of the target object.
     * @param config - Configuration source.
     * @param target - Target to apply the values to.
     * @param path   - Node path to search under.
     * @param <T>    - Annotated object type.
     * @return - Updated target instance.
     * @throws ConfigurationException
     */
    public static <T> T readConfigAnnotations(@Nonnull Class<? extends T> type,
                                              @Nonnull Configuration config,
                                              @Nonnull T target,
                                              String path,
                                              List<String> valuePaths)
            throws ConfigurationException {
        Preconditions.checkArgument(config != null);
        Preconditions.checkArgument(target != null);
        Preconditions.checkArgument(type != null);

        ConfigPath cPath = type.getAnnotation(ConfigPath.class);
        if (cPath != null) {
            if (!Strings.isNullOrEmpty(path)) {
                path = String.format("%s.%s", path, cPath.path());
            } else {
                path = cPath.path();
            }
            if (Strings.isNullOrEmpty(path)) {
                throw new ConfigurationException(
                        "Invalid Config Path : Path is NULL/Empty");
            }
            AbstractConfigNode node = config.find(path);
            if (node == null) {
                throw new ConfigurationException(
                        String.format("Invalid Path : Path not found. [path=%s]",
                                path));
            }
            processType(type, node, target, valuePaths);
        }
        return target;
    }

    /**
     * Reader and apply the values from the passed configuration based on the type annotations.
     *
     * @param type   - Type of the target object.
     * @param config - Configuration node source.
     * @param target - Target to apply the values to.
     * @param <T>    - Annotated object type.
     * @return - Updated target instance.
     * @throws ConfigurationException
     */
    public static <T> T readConfigAnnotations(@Nonnull Class<? extends T> type,
                                              @Nonnull ConfigPathNode config,
                                              @Nonnull T target)
            throws ConfigurationException {
        return readConfigAnnotations(type, config, target, null);
    }

    /**
     * Reader and apply the values from the passed configuration based on the type annotations.
     *
     * @param type   - Type of the target object.
     * @param config - Configuration node source.
     * @param target - Target to apply the values to.
     * @param <T>    - Annotated object type.
     * @return - Updated target instance.
     * @throws ConfigurationException
     */
    public static <T> T readConfigAnnotations(@Nonnull Class<? extends T> type,
                                              @Nonnull ConfigPathNode config,
                                              @Nonnull T target,
                                              List<String> valuePaths)
            throws ConfigurationException {
        Preconditions.checkArgument(config != null);
        Preconditions.checkArgument(target != null);
        Preconditions.checkArgument(type != null);

        ConfigPath cPath = type.getAnnotation(ConfigPath.class);
        if (cPath != null) {
            String path = cPath.path();
            if (Strings.isNullOrEmpty(path)) {
                throw new ConfigurationException(
                        "Invalid Config Path : Path is NULL/Empty");
            }
            AbstractConfigNode node = config.find(path);
            if (node == null) {
                throw new ConfigurationException(String.format("Configuration node not found. [node path=%s][search=%s]", config.getAbsolutePath(), cPath));
            }
            processType(type, node, target, valuePaths);
        }
        return target;
    }

    /**
     * Create a new instance of the type, will invoke an annotated constructor, if present, else
     * will try to invoke the default (empty) constructor.
     * <p>
     * Read and apply the values from the passed configuration based on the type annotations.
     *
     * @param type   - Type of the target object.
     * @param config - Configuration source.
     * @param path   - Node path to search under.
     * @param <T>    - Annotated object type.
     * @return - Updated target instance.
     * @throws ConfigurationException
     */
    public static <T> T readConfigAnnotations(@Nonnull Class<? extends T> type,
                                              @Nonnull Configuration config,
                                              String path)
            throws ConfigurationException {
        Preconditions.checkArgument(config != null);
        Preconditions.checkArgument(type != null);


        ConfigPath cPath = type.getAnnotation(ConfigPath.class);
        if (cPath != null) {
            if (!Strings.isNullOrEmpty(path)) {
                path = String.format("%s.%s", path, cPath.path());
            } else {
                path = cPath.path();
            }
            if (Strings.isNullOrEmpty(path)) {
                throw new ConfigurationException(
                        "Invalid Config Path : Path is NULL/Empty");
            }
            AbstractConfigNode node = config.find(path);
            if (node == null) {
                throw new ConfigurationException(
                        String.format("Invalid Path : Path not found. [path=%s]",
                                path));
            }
            T target = createInstance(type, node);
            return readConfigAnnotations(type, config, target, path, null);
        }
        throw new ConfigurationException(
                String.format("Path Annotation not found. [type=%s]",
                        type.getCanonicalName()));
    }

    /**
     * Create a new instance of the type, will invoke an annotated constructor, if present, else
     * will try to invoke the default (empty) constructor.
     * Read and apply the values from the passed configuration based on the type annotations.
     *
     * @param type   - Type of the target object.
     * @param config - Configuration source.
     * @param <T>    - Annotated object type.
     * @return - Updated target instance.
     * @throws ConfigurationException
     */
    public static <T> T readConfigAnnotations(@Nonnull Class<? extends T> type,
                                              @Nonnull Configuration config)
            throws ConfigurationException {
        Preconditions.checkArgument(config != null);
        Preconditions.checkArgument(type != null);


        ConfigPath cPath = type.getAnnotation(ConfigPath.class);
        if (cPath != null) {
            String path = cPath.path();
            if (Strings.isNullOrEmpty(path)) {
                throw new ConfigurationException(
                        "Invalid Config Path : Path is NULL/Empty");
            }
            AbstractConfigNode node = config.find(path);
            if (node == null) {
                throw new ConfigurationException(
                        String.format("Invalid Path : Path not found. [path=%s]",
                                path));
            }
            T target = createInstance(type, node);
            return readConfigAnnotations(type, config, target);
        }
        throw new ConfigurationException(
                String.format("Path Annotation not found. [type=%s]",
                        type.getCanonicalName()));
    }

    /**
     * Create a new instance of the type, will invoke an annotated constructor, if present, else
     * will try to invoke the default (empty) constructor.
     * <p>
     * Read and apply the values from the passed configuration based on the type annotations.
     *
     * @param type   - Type of the target object.
     * @param config - Configuration node source.
     * @param <T>    - Annotated object type.
     * @return - Updated target instance.
     * @throws ConfigurationException
     */
    public static <T> T readConfigAnnotations(@Nonnull Class<? extends T> type,
                                              @Nonnull ConfigPathNode config)
            throws ConfigurationException {
        Preconditions.checkArgument(config != null);
        Preconditions.checkArgument(type != null);

        T target = createInstance(type, config);
        return readConfigAnnotations(type, config, target, null);
    }

    /**
     * Create a new instance of the specified type.
     * Will look for an annotated constructor, if not found will
     * try to invoke the default (empty) constructor.
     *
     * @param type - Type of the target object.
     * @param node - Configuration node source.
     * @param <T>  - Annotated object type.
     * @return - New Object instance
     * @throws ConfigurationException
     */
    @SuppressWarnings("unchecked")
    private static <T> T createInstance(Class<? extends T> type,
                                        AbstractConfigNode node)
            throws ConfigurationException {
        try {
            Constructor<?>[] constructors = type.getConstructors();
            Constructor<?> defaultConst = null;
            T target = null;
            for (Constructor<?> constr : constructors) {
                if (Modifier.isPublic(constr.getModifiers())) {
                    Parameter[] params = constr.getParameters();
                    if (params == null || params.length == 0) {
                        defaultConst = constr;
                    } else {
                        if (constr.isAnnotationPresent(MethodInvoke.class)) {
                            MethodInvoke mi =
                                    constr.getAnnotation(MethodInvoke.class);
                            if (!Strings.isNullOrEmpty(mi.path())) {
                                node = node.find(mi.path());
                            }
                            if (node == null) {
                                throw new ConfigurationException(String.format(
                                        "Configuration Node not found. [path=%s]",
                                        mi.path()));
                            }
                            Object[] input = null;
                            List<Object> values = new ArrayList<>();
                            for (Parameter param : params) {
                                Object value = getParamValue(type, node, param);
                                values.add(value);
                            }
                            input = values.toArray();
                            target = (T) constr.newInstance(input);

                            break;
                        }
                    }
                }
            }
            if (target == null && defaultConst != null) {
                target = type.newInstance();
            } else if (target == null) {
                throw new ConfigurationException(
                        String.format("No valid constructor found. [type=%s]",
                                type.getCanonicalName()));
            }
            return target;
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Check and apply any annotations for all fields and methods.
     *
     * @param type   - Type of the target object.
     * @param node   - Extracted configuration node.
     * @param target - Target to apply the values to.
     * @param <T>    - Annotated object type.
     * @throws ConfigurationException
     */
    private static <T> void processType(Class<? extends T> type,
                                        AbstractConfigNode node, T target, List<String> valuePaths)
            throws ConfigurationException {
        Field[] fields = ReflectionUtils.getAllFields(type);
        if (fields != null && fields.length > 0) {
            for (Field field : fields) {
                processField(type, node, target, field, valuePaths);
            }
        }
        Method[] methods = ReflectionUtils.getAllMethods(type);
        if (methods != null && methods.length > 0) {
            for (Method method : methods) {
                processMethod(type, node, target, method);
            }
        }
    }

    /**
     * Check if method(s) have been marked for auto-invoke and invoke them
     * with the configuration parameters.
     *
     * @param type   - Instance Type
     * @param node   - Configuration Node.
     * @param target - Target instance.
     * @param method - Method to check.
     * @param <T>    - Annotated object type.
     * @throws ConfigurationException
     */
    private static <T> void processMethod(Class<? extends T> type,
                                          AbstractConfigNode node, T target,
                                          Method method)
            throws ConfigurationException {
        if (method.isAnnotationPresent(MethodInvoke.class)) {
            MethodInvoke mi = method.getAnnotation(MethodInvoke.class);
            if (!Strings.isNullOrEmpty(mi.path())) {
                node = node.find(mi.path());
            }
            if (node == null) {
                throw new ConfigurationException(
                        String.format("Configuration Node Not Found : [path=%s]",
                                mi.path()));
            }

            try {
                Object[] input = null;
                Parameter[] params = method.getParameters();
                if (params != null && params.length > 0) {
                    List<Object> values = new ArrayList<>();
                    for (Parameter param : params) {
                        Object value = getParamValue(type, node, param);
                        values.add(value);
                    }
                    input = values.toArray();
                }
                MethodUtils.invokeMethod(target, method.getName(), input);
            } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
                throw new ConfigurationException(e);
            }
        }
    }

    /**
     * Get the value of the annotated parameter from the configuration node.
     *
     * @param type  - Instance Type
     * @param node  - Configuration Node.
     * @param param - Method/Constructor parameter.
     * @param <T>   - Annotated object type.
     * @return - Value extracted from configuration.
     * @throws ConfigurationException
     */
    private static <T> Object getParamValue(Class<? extends T> type,
                                            AbstractConfigNode node,
                                            Parameter param)
            throws ConfigurationException {
        if (param.isAnnotationPresent(ConfigParam.class)) {
            ConfigParam p = param.getAnnotation(ConfigParam.class);
            String pname = p.name();
            if (Strings.isNullOrEmpty(pname) &&
                    !param.getType().equals(AbstractConfigNode.class)) {
                pname = param.getName();
            }
            if (param.getType().equals(AbstractConfigNode.class)) {
                if (!Strings.isNullOrEmpty(pname)) {
                    node = node.find(pname);
                }
                return node;
            }
            if (!(node instanceof ConfigPathNode)) {
                throw new ConfigurationException(String.format(
                        "Invalid Configuration Node type. [path=%s][type=%s]",
                        node.getSearchPath(), node.getClass().getCanonicalName()));
            }

            ConfigPathNode pnode = (ConfigPathNode) node;
            ConfigParametersNode paramnode = pnode.parmeters();
            String value = null;
            if (paramnode != null) {
                ConfigValueNode cv = paramnode.getValue(pname);
                if (cv != null) {
                    if (cv.isEncrypted()) {
                        if (param.getType() == EncryptedValue.class) {
                            EncryptedValue ev = new EncryptedValue(cv);
                            return ev;
                        }
                    }
                    value = cv.getValue();
                }
            }
            if (Strings.isNullOrEmpty(value) && p.required()) {
                throw new ConfigurationException(String.format(
                        "Required Parameter Value not defined. [param=%s][type=%s]",
                        param.getName(), type.getCanonicalName()));
            }
            Object v = null;
            if (!Strings.isNullOrEmpty(value)) {
                v = ReflectionUtils.parseStringValue(param.getType(), value);
                if (v == null) {
                    throw new ConfigurationException(String.format(
                            "Error parsing parameter value. [type=%s][value=%s]",
                            param.getType().getCanonicalName(), value));
                }
            }
            return v;
        } else {
            throw new ConfigurationException(String.format(
                    "Parameter Annotation not defined. [param=%s][type=%s]",
                    param.getName(), type.getCanonicalName()));
        }
    }

    /**
     * Check and apply any annotations for the specified field.
     *
     * @param type   - Type of the target object.
     * @param node   - Extracted configuration node.
     * @param target - Target to apply the values to.
     * @param field  - Field to check and apply to.
     * @param <T>    - Annotated object type.
     * @throws ConfigurationException
     */
    private static <T> void processField(Class<? extends T> type,
                                         AbstractConfigNode node, T target,
                                         Field field, List<String> valuePaths)
            throws ConfigurationException {
        try {
            if (field.isAnnotationPresent(ConfigParam.class)) {
                ConfigParam param = field.getAnnotation(ConfigParam.class);
                processParam(param, field, node, target, valuePaths);
            } else if (field.isAnnotationPresent(ConfigAttribute.class)) {
                ConfigAttribute attr =
                        field.getAnnotation(ConfigAttribute.class);
                processAttributes(attr, field, node, target, valuePaths);
            } else if (field.isAnnotationPresent(ConfigValue.class)) {
                ConfigValue value = field.getAnnotation(ConfigValue.class);
                processValue(type, value, field, node, target, valuePaths);
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Process a Config Value annotation.
     *
     * @param type        - Target type.
     * @param configValue - Config Value annotation.
     * @param field       - Field to process.
     * @param node        - Configuration node.
     * @param target      - Target instance.
     * @param <T>         - Target Type
     * @throws ConfigurationException
     */
    @SuppressWarnings("unchecked")
    private static <T> void processValue(Class<? extends T> type,
                                         ConfigValue configValue, Field field,
                                         AbstractConfigNode node, T target, List<String> valuePaths)
            throws ConfigurationException {
        try {
            Class<? extends ITransformer> tt = configValue.transformer();
            Class<? extends ICustomParser> pp = configValue.parser();
            String name = configValue.name();
            if (Strings.isNullOrEmpty(name)) {
                name = field.getName();
            }
            if (!pp.equals(NullParser.class)) {
                ICustomParser<?> parser = pp.newInstance();
                Object pvalue = parser.parse(node, name);
                ReflectionUtils.setObjectValue(target, field, pvalue);
            } else if ((field.getType().isEnum() || canSetFieldType(field))) {
                String value = null;
                if (node instanceof ConfigPathNode) {
                    AbstractConfigNode fnode = node.find(name);
                    if (fnode != null) {
                        if (fnode instanceof ConfigValueNode) {
                            ConfigValueNode cv = (ConfigValueNode) fnode;
                            if (cv.isEncrypted()) {
                                throw new ConfigurationException(String.format(
                                        "Encrypted value cannot be used. [path=%s]",
                                        cv.getSearchPath()));
                            }
                            value = cv.getValue();
                        } else if (fnode instanceof ConfigListValueNode) {
                            setListValueFromNode(type,
                                    ((ConfigListValueNode) fnode),
                                    target, field);
                        }
                        if (valuePaths != null) {
                            valuePaths.add(fnode.getSearchPath());
                        }
                    }
                }
                if (!Strings.isNullOrEmpty(value)) {
                    ReflectionUtils
                            .setValueFromString(value, target, field);
                } else if (configValue.required()) {
                    throw new ConfigurationException(String.format(
                            "Required configuration value not specified: [path=%s][name=%s]",
                            node.getAbsolutePath(), name));
                }
            } else if (field.getType() == EncryptedValue.class) {
                ConfigValueNode vn = null;
                if (node instanceof ConfigPathNode) {
                    AbstractConfigNode fnode = node.find(name);
                    if (fnode != null) {
                        if (fnode instanceof ConfigValueNode) {
                            vn = (ConfigValueNode) fnode;
                            if (!vn.isEncrypted()) {
                                throw new ConfigurationException(String.format(
                                        "Non-Encrypted value cannot be used. [path=%s]",
                                        vn.getSearchPath()));
                            }
                        }
                        if (valuePaths != null) {
                            valuePaths.add(fnode.getSearchPath());
                        }
                    }
                }
                if (vn == null) {
                    if (configValue.required()) {
                        throw new ConfigurationException(String.format(
                                "Required parameter not specified: [path=%s][name=%s]",
                                node.getAbsolutePath(), node.getName()));
                    }
                } else {
                    EncryptedValue ev = new EncryptedValue(vn);
                    ReflectionUtils.setObjectValue(target, field, ev);
                }
            } else {
                if (tt != NullTransformer.class) {
                    ITransformer<?, String> transformer = tt.newInstance();
                    String value = null;
                    if (node instanceof ConfigPathNode) {
                        AbstractConfigNode fnode = node.find(name);
                        if (fnode != null) {
                            if (fnode instanceof ConfigValueNode) {
                                ConfigValueNode cv = (ConfigValueNode) fnode;
                                value = cv.getValue();
                            }
                            if (valuePaths != null) {
                                valuePaths.add(fnode.getSearchPath());
                            }
                        }
                    }
                    if (!Strings.isNullOrEmpty(value)) {
                        Object tValue = transformer.transform(value);
                        ReflectionUtils.setObjectValue(target, field, tValue);
                    } else if (configValue.required()) {
                        throw new ConfigurationException(String.format(
                                "Required configuration value not specified: [path=%s][name=%s]",
                                node.getAbsolutePath(), name));
                    }
                } else if (!pp.equals(NullParser.class)) {
                    ICustomParser<?> parser = pp.newInstance();
                    Object pvalue = parser.parse(node, name);
                    ReflectionUtils.setObjectValue(target, field, pvalue);
                } else {
                    Class<?> ftype = field.getType();
                    String path = hasConfigAnnotation(ftype);
                    if (!Strings.isNullOrEmpty(path) &&
                            (node instanceof ConfigPathNode)) {
                        if (path.equals(".")) {
                            path = name;
                        } else {
                            path = String.format("%s.%s", path, name);
                        }
                        AbstractConfigNode cnode = node.find(path);
                        if (cnode != null &&
                                (cnode instanceof ConfigPathNode)) {
                            Object value = ftype.newInstance();
                            value = readConfigAnnotations(ftype,
                                    (ConfigPathNode) cnode,
                                    value, valuePaths);
                            ReflectionUtils
                                    .setObjectValue(target, field, value);
                            if (valuePaths != null) {
                                valuePaths.add(cnode.getSearchPath());
                            }
                        }
                        Object fv = ReflectionUtils.getFieldValue(target, field);
                        if (fv == null && configValue.required()) {
                            throw new ConfigurationException(String.format(
                                    "Required configuration value not specified: [path=%s][name=%s]",
                                    node.getAbsolutePath(), name));
                        }
                    } else {
                        throw new ConfigurationException(String.format(
                                "Parameter cannot be set for field of type = %s",
                                field.getType().getCanonicalName()));
                    }
                }
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Process a Config Parameter annotation.
     *
     * @param param  - Config Parameter annotation.
     * @param field  - Field to process.
     * @param node   - Configuration node.
     * @param target - Target instance.
     * @param <T>    - Target Type
     * @throws ConfigurationException
     */
    @SuppressWarnings("unchecked")
    private static <T> void processParam(ConfigParam param, Field field,
                                         AbstractConfigNode node, T target, List<String> valuePaths)
            throws ConfigurationException {
        try {
            String name = param.name();
            if (Strings.isNullOrEmpty(name)) {
                name = field.getName();
            }
            StructNodeInfo nodeInfo = checkAnnotationTags(name, node,
                    ConfigParametersNode.NODE_ABBR_PREFIX);
            if (field.getType() == EncryptedValue.class) {
                ConfigValueNode vn = null;
                if (node instanceof ConfigPathNode) {
                    ConfigPathNode pathNode = (ConfigPathNode) nodeInfo.node;
                    ConfigParametersNode params = pathNode.parmeters();
                    if (params != null && !params.isEmpty()) {
                        if (params.hasKey(nodeInfo.name))
                            vn = params.getValue(nodeInfo.name);
                    }
                    if (valuePaths != null) {
                        valuePaths.add(node.getSearchPath());
                    }
                }
                if (vn == null) {
                    if (param.required()) {
                        throw new ConfigurationException(String.format(
                                "Required parameter not specified: [path=%s][name=%s]",
                                node.getAbsolutePath(), nodeInfo.name));
                    }
                } else {
                    EncryptedValue ev = new EncryptedValue(vn);
                    ReflectionUtils.setObjectValue(target, field, ev);
                }

                return;
            }
            String value = null;
            if (node instanceof ConfigPathNode) {
                ConfigPathNode pathNode = (ConfigPathNode) nodeInfo.node;
                ConfigParametersNode params = pathNode.parmeters();
                if (params != null && !params.isEmpty()) {
                    if (params.hasKey(nodeInfo.name)) {
                        ConfigValueNode vn = params.getValue(nodeInfo.name);
                        if (vn != null) {
                            if (vn.isEncrypted()) {
                                throw new ConfigurationException(String.format(
                                        "Encrypted value cannot be used. [path=%s]",
                                        vn.getSearchPath()));
                            }
                            value = vn.getValue();
                        }
                    }
                }
                if (valuePaths != null) {
                    valuePaths.add(pathNode.getSearchPath());
                }
            }

            if (!Strings.isNullOrEmpty(value)) {
                if (canProcessFieldType(field) || field.getType().isEnum() || field.getType().equals(Class.class)) {
                    ReflectionUtils.setValueFromString(value, target, field);
                } else {
                    Class<? extends ITransformer> tt = param.transformer();
                    if (tt != NullTransformer.class) {
                        ITransformer<?, String> transformer = tt.newInstance();

                        Object tValue = transformer.transform(value);
                        ReflectionUtils.setObjectValue(target, field, tValue);
                    }
                }
            } else if (param.required()) {
                throw new ConfigurationException(String.format(
                        "Required parameter not specified: [path=%s][name=%s]",
                        node.getAbsolutePath(), nodeInfo.name));
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Process a Config Attribute annotation.
     *
     * @param attribute - Config Attribute annotation.
     * @param field     - Field to process.
     * @param node      - Configuration node.
     * @param target    - Target instance.
     * @param <T>       - Target Type
     * @throws ConfigurationException
     */
    @SuppressWarnings("unchecked")
    private static <T> void processAttributes(ConfigAttribute attribute,
                                              Field field,
                                              AbstractConfigNode node, T target, List<String> valuePaths)
            throws ConfigurationException {
        try {
            String name = attribute.name();
            if (Strings.isNullOrEmpty(name)) {
                name = field.getName();
            }
            StructNodeInfo nodeInfo = checkAnnotationTags(name, node,
                    ConfigAttributesNode.NODE_ABBR_PREFIX);
            if (field.getType() == EncryptedValue.class) {
                ConfigValueNode vn = null;
                if (node instanceof ConfigPathNode) {
                    ConfigPathNode pathNode = (ConfigPathNode) nodeInfo.node;
                    ConfigAttributesNode attrs = pathNode.attributes();
                    if (attrs != null && !attrs.isEmpty()) {
                        if (attrs.hasKey(nodeInfo.name))
                            vn = attrs.getValue(nodeInfo.name);
                    }
                    if (valuePaths != null) {
                        valuePaths.add(node.getSearchPath());
                    }
                }
                if (vn == null) {
                    if (attribute.required()) {
                        throw new ConfigurationException(String.format(
                                "Required parameter not specified: [path=%s][name=%s]",
                                node.getAbsolutePath(), nodeInfo.name));
                    }
                } else {
                    EncryptedValue ev = new EncryptedValue(vn);
                    ReflectionUtils.setObjectValue(target, field, ev);
                }

                return;
            }

            String value = null;
            if (node instanceof ConfigPathNode) {
                ConfigPathNode pathNode = (ConfigPathNode) nodeInfo.node;
                ConfigAttributesNode attrs = pathNode.attributes();
                if (attrs != null && !attrs.isEmpty()) {
                    if (attrs.hasKey(nodeInfo.name)) {
                        ConfigValueNode vn = attrs.getValue(nodeInfo.name);
                        if (vn != null) {
                            if (vn.isEncrypted()) {
                                throw new ConfigurationException(String.format(
                                        "Encrypted value cannot be used. [path=%s]",
                                        vn.getSearchPath()));
                            }
                            value = vn.getValue();
                        }
                    }
                }
                if (valuePaths != null) {
                    valuePaths.add(node.getSearchPath());
                }
            }
            if (!Strings.isNullOrEmpty(value)) {
                if (canProcessFieldType(field) || field.getType().isEnum() || field.getType().equals(Class.class)) {
                    ReflectionUtils.setValueFromString(value, target, field);
                } else {
                    Class<? extends ITransformer> tt = attribute.transformer();
                    if (tt != NullTransformer.class) {
                        ITransformer<?, String> transformer = tt.newInstance();

                        Object tValue = transformer.transform(value);
                        ReflectionUtils.setObjectValue(target, field, tValue);
                    }
                }
            } else if (attribute.required()) {
                throw new ConfigurationException(String.format(
                        "Required parameter not specified: [path=%s][name=%s]",
                        node.getAbsolutePath(), nodeInfo.name));
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Check field for annotation tag.
     *
     * @param name - Annotation name property.
     * @param node - Configuration node.
     * @param tag  - Annotation tag.
     * @return - Processed Name/Node.
     * @throws ConfigurationException
     */
    private static StructNodeInfo checkAnnotationTags(String name,
                                                      AbstractConfigNode node,
                                                      String tag)
            throws ConfigurationException {
        StructNodeInfo ni = new StructNodeInfo();
        ni.name = name;
        ni.node = node;
        if (name.contains(tag)) {
            String[] parts =
                    name.split(tag);
            if (parts.length == 2) {
                String path = parts[0];
                String nname = parts[1];
                if (Strings.isNullOrEmpty(path)) {
                    ni.name = nname;
                } else {
                    ni.node = node.find(path);
                    if (ni.node == null) {
                        throw new ConfigurationException(
                                String.format(
                                        "Invalid ConfigParam : path not found. [name=%s]",
                                        name));
                    }
                    ni.name = nname;
                }
            } else {
                throw new ConfigurationException(
                        String.format("Invalid ConfigParam : [name=%s]",
                                name));
            }
        }
        return ni;
    }

    /**
     * Set the value of the Collection type based on the passed Value list.
     *
     * @param type          - Class type of the target object.
     * @param listValueNode - List Value node to extract values from.
     * @param target        - Target object to set the values for
     * @param field         - Field in the target object.
     * @param <T>           - Generic type.
     * @throws ConfigurationException
     */
    private static <T> void setListValueFromNode(Class<? extends T> type,
                                                 ConfigListValueNode listValueNode,
                                                 T target, Field field)
            throws ConfigurationException {
        List<String> values = new ArrayList<>(listValueNode.size());
        List<ConfigValueNode> nodes = listValueNode.getValues();
        for (ConfigValueNode cvn : nodes) {
            values.add(cvn.getValue());
        }
        try {
            if (ReflectionUtils.implementsInterface(List.class, field.getType())) {
                CollectionUtils.setListValues(target, field, values);
            } else if (ReflectionUtils
                    .implementsInterface(Set.class, field.getType())) {
                CollectionUtils.setSetValues(target, field, values);
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Check if the specified annotated field can be processed.
     *
     * @param field - Field to check for.
     * @return - Can process?
     * @throws Exception
     */
    private static boolean canProcessFieldType(Field field) throws Exception {
        if (ReflectionUtils.isPrimitiveTypeOrString(field)) {
            return true;
        } else if (canSetFieldType(field)) {
            return true;
        } else {
            Class<?> type = field.getType();
            String ann = hasConfigAnnotation(type);
            if (!Strings.isNullOrEmpty(ann)) {
                return true;
            }
        }
        return false;
    }

    private static boolean canSetFieldType(Field field) throws Exception {
        if (ReflectionUtils.isPrimitiveTypeOrString(field) ||
                field.getType().isEnum()) {
            return true;
        } else if (field.getType().equals(Class.class)) {
            return true;
        }
        return false;
    }

    /**
     * Check and get the path annotation from the passed type.
     *
     * @param type - Type to check annotation for.
     * @param <T>  - Generic type.
     * @return - Config Path annotation value.
     */
    public static <T> String hasConfigAnnotation(Class<? extends T> type) {
        ConfigPath cPath = type.getAnnotation(ConfigPath.class);
        if (cPath != null) {
            return cPath.path();
        }
        return null;
    }

    /**
     * Get all the field configuration annotations for this type.
     *
     * @param type - Type to get annotation for.
     * @param <T>  - Generic type.
     * @return - List of field/annotation
     */
    public static <T> List<StructFieldAnnotation> getFieldAnnotations(
            Class<? extends T> type) {
        String path = hasConfigAnnotation(type);
        if (!Strings.isNullOrEmpty(path)) {
            List<StructFieldAnnotation> annotations = new ArrayList<>();
            Field[] fields = ReflectionUtils.getAllFields(type);
            if (fields != null && fields.length > 0) {
                for (Field field : fields) {
                    if (field.isAnnotationPresent(ConfigParam.class)) {
                        ConfigParam param = field.getAnnotation(ConfigParam.class);
                        StructFieldAnnotation fa = new StructFieldAnnotation();
                        fa.field = field;
                        fa.annotation = param;
                        annotations.add(fa);
                    } else if (field.isAnnotationPresent(ConfigValue.class)) {
                        ConfigValue param = field.getAnnotation(ConfigValue.class);
                        StructFieldAnnotation fa = new StructFieldAnnotation();
                        fa.field = field;
                        fa.annotation = param;
                        annotations.add(fa);
                    } else if (field.isAnnotationPresent(ConfigAttribute.class)) {
                        ConfigAttribute param =
                                field.getAnnotation(ConfigAttribute.class);
                        StructFieldAnnotation fa = new StructFieldAnnotation();
                        fa.field = field;
                        fa.annotation = param;
                        annotations.add(fa);
                    }
                }
            }
            if (!annotations.isEmpty()) {
                return annotations;
            }
        }
        return null;
    }

    /**
     * Struct class to pass information about field annotations.
     */
    public static class StructFieldAnnotation {
        /**
         * Annotated field.
         */
        public Field field;
        /**
         * Annotation type.
         */
        public Annotation annotation;
    }

    private static class StructNodeInfo {
        public String name;
        public AbstractConfigNode node;
    }
}
