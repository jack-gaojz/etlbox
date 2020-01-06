﻿using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Reads data from a memory source. While reading the data from the list, data is also asnychronously posted into the targets.
    /// Data is read a as string from the source and dynamically converted into the corresponding data format.
    /// </summary>
    public class MemorySource<TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Read data from memory";

        /* Public properties */
        public List<TOutput> Data { get; set; }

        /* Private stuff */

        public MemorySource() : base()
        {
            Data = new List<TOutput>();
        }

        public MemorySource(List<TOutput> data) : base()
        {
            Data = data;
        }

        public override void Execute()
        {
            NLogStart();
            ReadRecordAndSendIntoBuffer();
            LogProgress(1);
            Buffer.Complete();
            NLogFinish();
        }

        private void ReadRecordAndSendIntoBuffer()
        {
            foreach (TOutput record in Data)
            {
                Buffer.Post(record);
            }
        }

    }

    /// <summary>
    /// Reads data from a memory source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// MemorySource as a nongeneric type always return a string array as output. If you need typed output, use
    /// the MemorySource&lt;TOutput&gt; object instead.
    /// </summary>
    /// <see cref="MemorySource{TOutput}"/>
    public class MemorySource : MemorySource<string[]>
    {
        public MemorySource() : base() { }
        public MemorySource(List<string[]> data) : base(data) { }
    }
}