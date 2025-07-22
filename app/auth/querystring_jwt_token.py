from rest_framework_simplejwt.authentication import JWTAuthentication


class QueryStringJWTAuthentication(JWTAuthentication):
    def authenticate(self, request):
        raw_token = request.query_params.get("token")
        if raw_token is None:
            return None
        validated_token = self.get_validated_token(raw_token.replace(r"Bearer\s?", ""))
        return self.get_user(validated_token), validated_token
