import React, { useState, useEffect} from "react";
import WaitListForm from "@/features/auth/WaitListForm";

const SignupPage: React.FC = () => {
  const [socialUser, setSocialUser] = useState<{
    email: string;
    first_name: string;
    last_name: string;
  } | null>(null);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const token = params.get("token");
  
    if (token) {
      fetch(`/api/social-signup-token/?token=${token}`)
        .then(res => res.status === 204 ? null : res.json())
        .then(data => {
          if (data?.email) {
            setSocialUser(data);
          }
        })
        .catch(() => {});
    }
  }, []);

  return <WaitListForm user={socialUser ?? undefined} />;
};

export default SignupPage;