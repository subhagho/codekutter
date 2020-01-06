/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.codekutter.zconfig.common.transformers;

import com.codekutter.zconfig.common.model.annotations.ITransformer;

/**
 * No op transformer defined as default for annotations.
 */
public class NullTransformer implements ITransformer<Object, Object> {
    /**
     * Transform the source value to the target type.
     *
     * @param source - Source value.
     * @return - Transformed value.
     * @throws TransformationException
     */
    @Override
    public Object transform(Object source) throws TransformationException {
        throw new TransformationException("Method not implemented.");
    }

    /**
     * Transform the target value to the source type.
     *
     * @param source - Source value.
     * @return - Transformed value.
     * @throws TransformationException
     */
    @Override
    public Object reverse(Object source) throws TransformationException {
        throw new TransformationException("Method not implemented.");
    }
}
