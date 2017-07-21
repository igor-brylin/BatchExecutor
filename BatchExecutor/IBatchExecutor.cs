using System;
using System.Threading.Tasks;

namespace BatchExecutor
{
	public interface IBatchExecutor<in TItem, TResult> : IDisposable
	{
		Task<TResult> ExecAsync(TItem i);
	}
}