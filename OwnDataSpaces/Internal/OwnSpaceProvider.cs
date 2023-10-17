using Microsoft.AspNetCore.Http;

namespace OwnDataSpaces.Internal;

internal class OwnSpaceProvider
{
    private readonly IHttpContextAccessor _contextAccessor;
    private Guid? _spaceId;

    public OwnSpaceProvider(IHttpContextAccessor contextAccessor)
    {
        _contextAccessor = contextAccessor;
    }


    internal void SetSpaceId(Guid spaceId)
    {
        _spaceId = spaceId;
    }

    public Guid GetSpaceId()
    {
        if (_spaceId is not null)
        {
            return _spaceId.Value;
        }

        if (_contextAccessor.HttpContext?.Request.Headers.TryGetValue("OwnSpaceId", out var ownSpaceId) == true)
        {
            return Guid.Parse(ownSpaceId[0]!);
        }

        throw new InvalidOperationException(
            "Could not get OwnSpaceId from HttpContext as well as from explicitly set value");
    }
}