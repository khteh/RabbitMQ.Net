using Microsoft.Extensions.DependencyInjection;

namespace RabbitMQ.UnitTests;

public sealed class TestAppInitializer
{
    //Startup _startUp;
    public TestAppInitializer()
    {
        ServiceCollection services = new ServiceCollection();
        services.Add
    }

    public override IDependencyContainer Build()
    {
        if (_startUp == null)
            _startUp = new Startup(null);
        _startUp.Initialize(this);

        var container = base.Build();
        AddShutdownAction(() => container.Dispose());

        return container;
    }
}