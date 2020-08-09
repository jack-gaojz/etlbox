using ETLBox.ControlFlow;
using System;
using System.Dynamic;
using System.Globalization;
using TheBoxOffice.LicenseManager;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Writes data into a text file. Each line is created by calling the LineSelector Func or by invoking ToString() on the object.
    /// </summary>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    public class TextDestination<TInput> : DataFlowStreamDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => $"Write text data into file {Uri ?? ""}";

        public Func<TInput, string> LineSelector { get; set; }

        public TextDestination()
        {
            ResourceType = ResourceType.File;
        }

        public TextDestination(string filename) : this()
        {
            Uri = filename;
        }

        public TextDestination(string filename, Func<TInput, string> lineSelector) : this(filename)
        {
            LineSelector = lineSelector;
        }

        protected override void InitStream()
        {
        }

        protected override void WriteIntoStream(TInput data)
        {
            WriteObject(data);
            LogProgress();
        }

        private void WriteObject(TInput data)
        {
            if (data == null) return;
            try
            {
                string line;
                if (LineSelector != null)
                    line = LineSelector?.Invoke(data);
                else
                    line = data.ToString();
                StreamWriter.WriteLine(line);
            }
            catch (Exception e)
            {
                ThrowOrRedirectError(e, ErrorSource.ConvertErrorData(data));
            }
        }

        protected override void CloseStream()
        {
        }
    }

    /// <summary>
    /// Writes data into a text file. Each line is created by calling the LineSelector Func or by invoking ToString() on the object.
    /// </summary>
    public class TextDestination : TextDestination<ExpandoObject>
    {
        public TextDestination() : base() { }

        public TextDestination(string fileName) : base(fileName) { }

        public TextDestination(string fileName, Func<ExpandoObject, string> lineSelector) : base(fileName, lineSelector) { }

    }

}
