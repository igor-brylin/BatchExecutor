using System;

namespace BatchExecutor
{
	internal class WorkItem<TItem, TResult>
	{
		public TItem DataItem { get; set; }
		public Action<Exception, TResult> Callback { get; set; }
	}
}