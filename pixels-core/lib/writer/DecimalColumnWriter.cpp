/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
#include "writer/DecimalColumnWriter.h"
#include "utils/BitUtils.h"

DecimalColumnWriter::DecimalColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption) :
ColumnWriter(type, writerOption)
{
}

//why???

int DecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length)
{
    std::cout<<"DecimalColumnWriter::write"<<std::endl;
    auto columnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
    EncodingUtils encodingUtils;
    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }
    long *values = columnVector->vector;    
    for(int i=0;i<length;i++){
        isNull[curPixelIsNullIndex]=columnVector->isNull[i];
        std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
        curPixelEleIndex++;
        if(byteOrder==ByteOrder::PIXELS_LITTLE_ENDIAN){
            if(nullsPadding){
                hasNull=true;   
                encodingUtils.writeLongLE(curVecPartitionBuffer, 0l);
            }
            else{
                encodingUtils.writeLongLE(outputStream, values[i]);
            }
        }
        else{
            if(nullsPadding){
                hasNull=true;   
                encodingUtils.writeLongBE(curVecPartitionBuffer, 0l);
            }
            else{
                encodingUtils.writeLongBE(outputStream, values[i]);
            }
        }
        if(curPixelEleIndex>=pixelStride){
            newPixel();
        }
    }
    return outputStream->getWritePos();
}

//why??

bool DecimalColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption)
{
    return writerOption->isNullsPadding();
}