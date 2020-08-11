namespace ETLBox.DataFlow
{
    public interface IDataFlowTransformation<TInput, TOutput> : IDataFlowSource<TOutput>, IDataFlowDestination<TInput>
    {

    }

    public interface IDataFlowTransformation<TOutput> : IDataFlowSource<TOutput>, IDataFlowDestination
    {

    }
}
