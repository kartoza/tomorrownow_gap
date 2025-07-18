export interface User {
    id: number;
    username: string;
    first_name: string;
    last_name: string;
    email: string;
    is_staff: boolean;
    is_superuser: boolean;
}

// Router location state type
export interface LocationState {
  from?: {
    pathname: string;
  };
}

