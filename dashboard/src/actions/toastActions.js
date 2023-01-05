import * as TYPES from "./types";

import { logout } from "./authActions";
import { uid } from "utils/helper";

export const showSessionExpired = () => async (dispatch) => {
  const toast = {
    variant: "danger",
    title: "Session Expired",
    message: "Please login to continue",
  };
  dispatch(showToast(toast.variant, toast.title, toast.message));
  dispatch(logout());
};

export const showFailureToast = () => async (dispatch) => {
  const toast = {
    variant: "danger",
    title: "Something went wrong",
    message: "Please try again later",
  };
  dispatch(showToast(toast.variant, toast.title, toast.message));
};

export const showToast =
  (variant, title, message = "") =>
  (dispatch, getState) => {
    const obj = {
      variant: variant,
      title: title,
      message: message,
      key: uid(),
    };
    const alerts = [...getState().toastReducer.alerts, obj];

    dispatch({
      type: TYPES.SHOW_TOAST,
      payload: alerts,
    });
  };

export const hideToast = (key) => (dispatch, getState) => {
  const alerts = getState().toastReducer.alerts;
  const activeAlert = alerts.filter((item) => item.key !== key);

  dispatch({
    type: TYPES.SHOW_TOAST,
    payload: activeAlert,
  });
};

export const clearToast = () => (dispatch) => {
  dispatch({ type: TYPES.CLEAR_TOAST });
};
