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
 * Date: 1/1/19 8:56 PM
 * Subho Ghosh (subho dot ghosh at outlook.com)
 *
 */

package com.codekutter.zconfig.common.model;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.codekutter.zconfig.common.ValueParseException;

import javax.annotation.Nonnull;

/**
 * Class represents an asset version.
 * <p>
 * Version: {Major Version, Minor Version}
 */
public class Version {
    public static final int MATCH_ALL_MARKER = -9999;

    /**
     * Major version number - changes in major version numbers are assumed to break
     * backward compatibility.
     */
    private int majorVersion = 0;

    /**
     * Minor version number - changes in minor version numbers are NOT expected to break
     * backward compatibility.
     */
    private int minorVersion = 0;

    /**
     * Default empty constructor
     */
    public Version() {

    }

    /**
     * Initializing constructor with version data.
     *
     * @param majorVersion - Major Version
     * @param minorVersion - Minor Version
     */
    public Version(int majorVersion, int minorVersion) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    /**
     * Get the Major Version number.
     *
     * @return - Major version number.
     */
    public int getMajorVersion() {
        return majorVersion;
    }

    /**
     * Set the Major Version number.
     *
     * @param majorVersion - Major Version number
     */
    public void setMajorVersion(int majorVersion) {
        Preconditions.checkArgument(majorVersion >= 0);
        this.majorVersion = majorVersion;
    }

    /**
     * Get the Minor Version number.
     *
     * @return - Minor Version number.
     */
    public int getMinorVersion() {
        return minorVersion;
    }

    /**
     * Set the Minor Version number.
     *
     * @param minorVersion - Minor Version number.
     */
    public void setMinorVersion(int minorVersion) {
        Preconditions.checkArgument(minorVersion >= 0);
        this.minorVersion = minorVersion;
    }

    /**
     * Check version compatibility. Versions are assumed to be compatible if the
     * majorVersion is the same.
     *
     * @param source - Source Version to compare with.
     * @return - Is compatible?
     */
    public boolean isCompatible(@Nonnull Version source) {
        Preconditions.checkArgument(source != null);
        if (majorVersion == source.majorVersion) {
            return true;
        }
        return false;
    }

    /**
     * Compare this version with the target version.
     *
     * <pre>
     *      if (this < target) return < 0
     *      if (this > target) return > 0
     *      else return 0
     * </pre>
     *
     * @param target - Target version to compare to.
     * @return - Comparison value
     */
    public int compare(@Nonnull Version target) {
        int ret = this.majorVersion - target.majorVersion;
        if (ret == 0) {
            ret = this.minorVersion - target.minorVersion;
        }
        return ret;
    }

    /**
     * Compare the Minor version with the target. Major version should be same else
     * will throw a runtime exception.
     * <pre>
     *      if (this < target) return < 0
     *      if (this > target) return > 0
     *      else return 0
     * </pre>
     *
     * @param target - Target version to compare to.
     * @return - Comparison value
     */
    public int compareMinorVersion(@Nonnull Version target) {
        Preconditions.checkArgument(majorVersion == target.minorVersion);
        return (minorVersion - target.minorVersion);
    }

    /**
     * Override the default toString to print the version info.
     *
     * @return - Version Info (majorVersion.minorVersion)
     */
    @Override
    public String toString() {
        return String.format("%s.%s", majorVersion, minorVersion);
    }

    /**
     * Override the default equals to compare the major/minor version numbers.
     *
     * @param o - Source version to compare to.
     * @return - Is equal?
     */
    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof Version) {
            Version v = (Version) o;
            if (this.minorVersion == MATCH_ALL_MARKER ||
                    v.minorVersion == MATCH_ALL_MARKER) {
                return (v.majorVersion == this.majorVersion);
            } else
                return (v.majorVersion == majorVersion &&
                        v.minorVersion == minorVersion);
        }
        return super.equals(o);
    }

    /**
     * Utility method to parse the Version object from the specified string value.
     *
     * @param value - String value of version (majorVersion.minorVersion)
     * @return - Parsed Version object.
     * @throws ValueParseException
     */
    public static final Version parse(@Nonnull String value)
    throws ValueParseException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        String[] parts = value.split("\\.");
        if (parts == null || parts.length < 2) {
            throw new ValueParseException(
                    String.format("Error parsing Version from string. [value=%s]",
                                  value));
        }
        Version version = new Version();
        version.majorVersion = Integer.parseInt(parts[0]);
        if (parts[1].trim().compareTo("*") == 0) {
            version.minorVersion = MATCH_ALL_MARKER;
        } else
            version.minorVersion = Integer.parseInt(parts[1]);

        return version;
    }
}
