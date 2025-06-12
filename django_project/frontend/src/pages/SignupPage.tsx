import React from "react";
import { useLocation } from "react-router-dom";
import WaitListForm from "@/features/auth/WaitListForm";

type LocationState = {
    incomplete_signup?: boolean;
    email: string;
    first_name?: string;
    last_name?: string;
  };

const SignupPage: React.FC = () => {
  const location = useLocation();
  // Cast because TS only knows location.state might be {}
  const state = location.state as LocationState | undefined;

  // If we got state from navigate(), pass it down; else undefined
  const user = state && state.incomplete_signup
    ? { 
        email: state.email,
        first_name: state.first_name,
        last_name: state.last_name
      }
    : undefined;
    return (
        <WaitListForm user={user}/>
    );
};

export default SignupPage;